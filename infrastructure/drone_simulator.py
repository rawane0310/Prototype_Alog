"""
EMSDN – Infrastructure Layer: Drone Simulator
Simulates drone fleet with GPS telemetry, battery, thermal payload monitoring.
Communication: MQTT publish-subscribe (simulated via threading + queues).
"""
import threading
import time
import random
import queue
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class DroneStatus(Enum):
    AVAILABLE = "available"
    IN_MISSION = "in_mission"
    CHARGING = "charging"
    MAINTENANCE = "maintenance"
    RETURNING = "returning"


@dataclass
class DroneSpec:
    drone_id: str
    max_range_km: float
    max_payload_kg: float
    position_depot: str  # e.g. "Chlef"
    battery_pct: float = 100.0
    status: DroneStatus = DroneStatus.AVAILABLE


@dataclass
class TelemetryPacket:
    drone_id: str
    timestamp: float
    gps_lat: float
    gps_lon: float
    payload_temp_celsius: float
    battery_pct: float
    altitude_m: float
    status: str
    anomaly: Optional[str] = None


class MQTTBrokerSimulator:
    """
    Simulated MQTT broker – publish/subscribe pattern.
    Drones publish to topics like 'telemetry/{drone_id}'.
    FlightMonitor subscribes to all active drone topics.
    """
    def __init__(self):
        self._subscribers: dict[str, list] = {}
        self._lock = threading.Lock()

    def subscribe(self, topic: str, callback):
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)

    def publish(self, topic: str, payload: TelemetryPacket):
        with self._lock:
            callbacks = self._subscribers.get(topic, [])
        for cb in callbacks:
            cb(payload)

    def unsubscribe(self, topic: str, callback):
        with self._lock:
            if topic in self._subscribers:
                try:
                    self._subscribers[topic].remove(callback)
                except ValueError:
                    pass


# Global broker instance (shared across the system)
mqtt_broker = MQTTBrokerSimulator()


class DroneUnit:
    """
    Simulates a physical drone executing a mission.
    Publishes telemetry every TELEMETRY_INTERVAL_SEC via MQTT.
    """
    TELEMETRY_INTERVAL_SEC = 0.3   # accelerated for demo (real: 5s)
    FLIGHT_SPEED_KM_MIN = 5.0       # accelerated (real: ~2 km/min)

    def __init__(self, spec: DroneSpec):
        self.spec = spec
        self._mission_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.delivery_confirmed = threading.Event()
        self.anomaly_injected = False

    def execute_mission(self, mission_id: str, distance_km: float,
                        supply_type: str, inject_anomaly: bool = False):
        """Start drone mission in background thread (Master-Slave pattern)."""
        self.spec.status = DroneStatus.IN_MISSION
        self._stop_event.clear()
        self.delivery_confirmed.clear()
        self.anomaly_injected = inject_anomaly

        self._mission_thread = threading.Thread(
            target=self._mission_loop,
            args=(mission_id, distance_km, supply_type),
            daemon=True
        )
        self._mission_thread.start()

    def _mission_loop(self, mission_id: str, distance_km: float, supply_type: str):
        flight_duration_sec = (distance_km / self.FLIGHT_SPEED_KM_MIN) * 60
        # Accelerate for demo
        flight_duration_sec = min(flight_duration_sec, 4.0)
        steps = int(flight_duration_sec / self.TELEMETRY_INTERVAL_SEC)
        topic = f"telemetry/{self.spec.drone_id}"

        base_lat, base_lon = 36.3, 1.8  # Chlef depot approx
        dest_lat, dest_lon = 36.26, 1.97  # Aïn Defla approx

        for i in range(steps):
            if self._stop_event.is_set():
                break

            progress = i / max(steps - 1, 1)
            lat = base_lat + (dest_lat - base_lat) * progress
            lon = base_lon + (dest_lon - base_lon) * progress

            # Simulate anomaly at 60% of flight if requested
            anomaly = None
            payload_temp = 4.0 + random.uniform(-0.3, 0.3)
            if self.anomaly_injected and 0.55 < progress < 0.70:
                payload_temp = 9.5 + random.uniform(0, 1.5)  # thermal breach
                anomaly = "PAYLOAD_TEMP_BREACH"

            battery = self.spec.battery_pct - (progress * 25)
            packet = TelemetryPacket(
                drone_id=self.spec.drone_id,
                timestamp=time.time(),
                gps_lat=round(lat, 5),
                gps_lon=round(lon, 5),
                payload_temp_celsius=round(payload_temp, 2),
                battery_pct=round(battery, 1),
                altitude_m=round(85 + random.uniform(-5, 5), 1),
                status="in_flight",
                anomaly=anomaly
            )
            mqtt_broker.publish(topic, packet)
            time.sleep(self.TELEMETRY_INTERVAL_SEC)

        # Land and confirm delivery
        if not self._stop_event.is_set():
            landing_packet = TelemetryPacket(
                drone_id=self.spec.drone_id,
                timestamp=time.time(),
                gps_lat=dest_lat, gps_lon=dest_lon,
                payload_temp_celsius=4.1,
                battery_pct=round(self.spec.battery_pct - 25, 1),
                altitude_m=0.0,
                status="landed"
            )
            mqtt_broker.publish(topic, landing_packet)
            self.spec.status = DroneStatus.RETURNING
            self.delivery_confirmed.set()

    def abort_mission(self):
        self._stop_event.set()
        self.spec.status = DroneStatus.RETURNING


# Pre-configured drone fleet
def create_drone_fleet() -> list[DroneUnit]:
    specs = [
        DroneSpec("D-05", max_range_km=60, max_payload_kg=3.0,
                  position_depot="Tiaret", battery_pct=72.0,
                  status=DroneStatus.CHARGING),
        DroneSpec("D-07", max_range_km=85, max_payload_kg=5.0,
                  position_depot="Chlef", battery_pct=98.0,
                  status=DroneStatus.AVAILABLE),
        DroneSpec("D-12", max_range_km=75, max_payload_kg=4.0,
                  position_depot="Chlef", battery_pct=45.0,
                  status=DroneStatus.MAINTENANCE),
        DroneSpec("D-03", max_range_km=90, max_payload_kg=5.0,
                  position_depot="Blida", battery_pct=88.0,
                  status=DroneStatus.AVAILABLE),
    ]
    return [DroneUnit(s) for s in specs]
