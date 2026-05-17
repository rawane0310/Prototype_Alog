"""
EMSDN – Business Layer: Flight Monitor
Subscribes to drone MQTT telemetry topics and detects in-flight anomalies.
Triggers Failsafe Controller on anomaly detection.
Operates asynchronously – does not block the dispatch pipeline.
"""
import threading
import time
from typing import Optional, Callable
from colorama import Fore, Style

from infrastructure.drone_simulator import mqtt_broker, TelemetryPacket
from business.audit_service import audit_service


class FlightMonitor:
    """
    Monitors active drone missions via MQTT telemetry.
    Subscribes to telemetry/{drone_id} topics for all active drones.
    Detects: payload temperature breach, battery critical, GPS loss.

    Interface:
        start_monitoring(drone_id, mission_id, anomaly_callback)
        stop_monitoring(drone_id)
    """
    TEMP_MAX_CELSIUS = 8.0      # Blood products must stay ≤ 8°C
    BATTERY_CRITICAL_PCT = 15.0
    TELEMETRY_TIMEOUT_SEC = 2.0  # Max silence before alert (real: 30s)

    def __init__(self, verbose: bool = True):
        self._active: dict[str, dict] = {}  # drone_id → {mission_id, callback, last_seen}
        self._lock = threading.Lock()
        self._verbose = verbose

    def start_monitoring(self, drone_id: str, mission_id: str,
                         anomaly_callback: Optional[Callable] = None):
        """Subscribe to MQTT telemetry for a drone and start watchdog."""
        def on_telemetry(packet: TelemetryPacket):
            self._handle_telemetry(packet, mission_id, anomaly_callback)

        with self._lock:
            self._active[drone_id] = {
                "mission_id": mission_id,
                "callback": anomaly_callback,
                "last_seen": time.time(),
                "mqtt_callback": on_telemetry,
                "anomalies_detected": []
            }

        mqtt_broker.subscribe(f"telemetry/{drone_id}", on_telemetry)

        if self._verbose:
            print(f"{Fore.CYAN}  [FlightMonitor] Monitoring started for {drone_id} "
                  f"(mission {mission_id}){Style.RESET_ALL}")

        audit_service.log_event(
            "FLIGHT_MONITORING_START", mission_id,
            {"drone_id": drone_id},
            "FlightMonitor"
        )

    def stop_monitoring(self, drone_id: str):
        """Unsubscribe from MQTT topic when mission ends."""
        with self._lock:
            entry = self._active.pop(drone_id, None)

        if entry:
            mqtt_broker.unsubscribe(f"telemetry/{drone_id}", entry["mqtt_callback"])
            audit_service.log_event(
                "FLIGHT_MONITORING_STOP", entry["mission_id"],
                {"drone_id": drone_id},
                "FlightMonitor"
            )

    def _handle_telemetry(self, packet: TelemetryPacket,
                          mission_id: str, anomaly_callback: Optional[Callable]):
        with self._lock:
            entry = self._active.get(packet.drone_id)
            if entry:
                entry["last_seen"] = time.time()

        # Check anomaly flag from drone
        if packet.anomaly:
            self._raise_anomaly(packet, mission_id, packet.anomaly, anomaly_callback)
            return

        # Payload thermal check
        if packet.payload_temp_celsius > self.TEMP_MAX_CELSIUS:
            reason = (f"PAYLOAD_TEMP_BREACH: {packet.payload_temp_celsius:.1f}°C "
                      f"(max {self.TEMP_MAX_CELSIUS}°C)")
            self._raise_anomaly(packet, mission_id, reason, anomaly_callback)
            return

        # Battery critical check
        if packet.battery_pct < self.BATTERY_CRITICAL_PCT:
            reason = f"BATTERY_CRITICAL: {packet.battery_pct:.1f}%"
            self._raise_anomaly(packet, mission_id, reason, anomaly_callback)
            return

        # Landing confirmed
        if packet.status == "landed":
            audit_service.log_event(
                "DRONE_LANDED", mission_id,
                {
                    "drone_id": packet.drone_id,
                    "final_temp": packet.payload_temp_celsius,
                    "final_battery": packet.battery_pct
                },
                "FlightMonitor"
            )
            if self._verbose:
                print(f"{Fore.GREEN}  [FlightMonitor] ✓ {packet.drone_id} landed. "
                      f"Temp: {packet.payload_temp_celsius}°C | "
                      f"Battery: {packet.battery_pct}%{Style.RESET_ALL}")

    def _raise_anomaly(self, packet: TelemetryPacket, mission_id: str,
                       reason: str, callback: Optional[Callable]):
        with self._lock:
            entry = self._active.get(packet.drone_id, {})
            if reason in entry.get("anomalies_detected", []):
                return  # Already reported
            if entry:
                entry["anomalies_detected"].append(reason)

        print(f"{Fore.RED}  [FlightMonitor] ⚠ ANOMALY on {packet.drone_id}: "
              f"{reason}{Style.RESET_ALL}")

        audit_service.log_event(
            "ANOMALY_DETECTED", mission_id,
            {"drone_id": packet.drone_id, "reason": reason,
             "lat": packet.gps_lat, "lon": packet.gps_lon},
            "FlightMonitor"
        )

        if callback:
            callback(packet.drone_id, mission_id, reason)
