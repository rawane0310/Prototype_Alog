"""
EMSDN – Business Layer: Failsafe Controller
Evaluates in-flight anomalies and decides response policy:
  - PAYLOAD_TEMP_BREACH → abort mission, return to base
  - BATTERY_CRITICAL → emergency landing at nearest safe point
  - GPS_LOSS → enter autonomous hover, await signal recovery

Directly notifies the clinic (bypassing layer hierarchy for emergency reactivity).
This is the deliberate "cross-layer shortcut" described in the architecture document.
"""
import time
from colorama import Fore, Style

from business.audit_service import audit_service
from business.notification_service import notification_service
from infrastructure.drone_simulator import DroneUnit


class FailsafeController:
    """
    Evaluates anomalies and triggers appropriate response.

    Interface:
        evaluate_anomaly(drone_id, mission_id, reason, drone, clinic_id)
        trigger_return(drone, mission_id)
    """

    def evaluate_anomaly(self, drone_id: str, mission_id: str,
                         reason: str, drone: DroneUnit,
                         clinic_id: str):
        """
        Central decision point for anomaly response.
        Note: Directly notifies clinic – intentional cross-layer communication
        as specified in the EMSDN architecture for emergency reactivity.
        """
        print(f"{Fore.YELLOW}  [FailsafeController] Evaluating anomaly: {reason}{Style.RESET_ALL}")

        audit_service.log_event(
            "FAILSAFE_EVALUATE", mission_id,
            {"drone_id": drone_id, "reason": reason},
            "FailsafeController"
        )

        if "PAYLOAD_TEMP" in reason or "TEMP_BREACH" in reason:
            self._handle_thermal_breach(drone, mission_id, clinic_id)
        elif "BATTERY_CRITICAL" in reason:
            self._handle_battery_critical(drone, mission_id, clinic_id)
        elif "GPS_LOSS" in reason:
            self._handle_gps_loss(drone, mission_id, clinic_id)
        else:
            # Unknown anomaly – default to abort
            self._handle_thermal_breach(drone, mission_id, clinic_id)

    def _handle_thermal_breach(self, drone: DroneUnit, mission_id: str, clinic_id: str):
        """Blood products compromised – abort mission immediately."""
        drone.abort_mission()
        time.sleep(0.1)

        audit_service.log_event(
            "MISSION_ABORTED_THERMAL", mission_id,
            {"drone_id": drone.spec.drone_id,
             "reason": "Payload temperature exceeded 8°C – blood products compromised"},
            "FailsafeController"
        )

        # Direct cross-layer notification to clinic (emergency bypass)
        notification_service.notify(
            clinic_id,
            f"⚠ ALERTE: Mission {mission_id} annulée. "
            f"Rupture chaîne du froid détectée sur {drone.spec.drone_id}. "
            f"Les produits sanguins sont compromis. Veuillez renouveler la demande.",
            mission_id,
            severity="critical"
        )

        print(f"{Fore.RED}  [FailsafeController] Mission ABORTÉE – "
              f"drone {drone.spec.drone_id} retour base.{Style.RESET_ALL}")

    def _handle_battery_critical(self, drone: DroneUnit, mission_id: str, clinic_id: str):
        """Battery too low – trigger emergency landing."""
        audit_service.log_event(
            "EMERGENCY_LANDING_TRIGGERED", mission_id,
            {"drone_id": drone.spec.drone_id, "battery": drone.spec.battery_pct},
            "FailsafeController"
        )
        notification_service.notify(
            clinic_id,
            f"⚠ Batterie critique sur {drone.spec.drone_id}. "
            f"Atterrissage d'urgence en cours. Mission {mission_id} suspendue.",
            mission_id,
            severity="critical"
        )

    def _handle_gps_loss(self, drone: DroneUnit, mission_id: str, clinic_id: str):
        """GPS signal lost – enter hover mode."""
        audit_service.log_event(
            "GPS_LOSS_HOVER", mission_id,
            {"drone_id": drone.spec.drone_id},
            "FailsafeController"
        )
        notification_service.notify(
            clinic_id,
            f"⚠ Signal GPS perdu sur {drone.spec.drone_id}. "
            f"Mode de vol autonome activé. Attente rétablissement signal.",
            mission_id,
            severity="warning"
        )
