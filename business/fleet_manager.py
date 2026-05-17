"""
EMSDN – Business Layer: Fleet Manager
Identifies the optimal available drone for a given mission.
Runs as a CONCURRENT THREAD in the dispatch pipeline (Introduce Concurrency tactic).
This is step 2b – runs in parallel with Inventory Matcher (step 2a).
"""
import threading
import time
from dataclasses import dataclass
from typing import Optional
import math

from infrastructure.drone_simulator import DroneUnit, DroneStatus
from business.audit_service import audit_service


@dataclass
class FleetSelectionResult:
    success: bool
    selected_drone: Optional[DroneUnit]
    distance_km: float
    duration_sec: float
    error_msg: Optional[str] = None


# Depot coordinates for distance calculation
DEPOT_COORDS = {
    "Chlef":  (36.165, 1.329),
    "Tiaret": (35.370, 1.320),
    "Blida":  (36.470, 2.826),
}

# Target (Aïn Defla clinic) for demo
TARGET_COORDS = (36.267, 1.967)


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Calculate great-circle distance in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


class FleetManager:
    """
    Manages drone fleet availability and mission assignment.

    Interface:
        get_available_drone_async(range_km, payload_kg, mission_id,
                                   result_holder, done_event) → thread
        get_available_drone_sync(range_km, payload_kg, mission_id) → blocking
        assign_mission(drone, mission_id) → bool
    """
    SIMULATED_LATENCY_SEC = 0.35  # DB lookup + telemetry check

    def __init__(self, fleet: list[DroneUnit]):
        self._fleet = fleet
        self._lock = threading.Lock()

    def get_available_drone_async(self, min_range_km: float, payload_kg: float,
                                   mission_id: str, result_holder: list,
                                   done_event: threading.Event):
        """Async version for parallel dispatch pipeline."""
        result = self._do_select(min_range_km, payload_kg, mission_id)
        result_holder.append(result)
        done_event.set()

    def get_available_drone_sync(self, min_range_km: float, payload_kg: float,
                                  mission_id: str) -> FleetSelectionResult:
        """Synchronous version – for benchmark sequential mode."""
        return self._do_select(min_range_km, payload_kg, mission_id)

    def _do_select(self, min_range_km: float, payload_kg: float,
                   mission_id: str) -> FleetSelectionResult:
        start = time.perf_counter()
        time.sleep(self.SIMULATED_LATENCY_SEC)  # Simulate fleet DB query

        audit_service.log_event(
            "FLEET_SELECTION_START", mission_id,
            {"min_range_km": min_range_km, "payload_kg": payload_kg},
            "FleetManager"
        )

        with self._lock:
            candidates = [
                d for d in self._fleet
                if (d.spec.status == DroneStatus.AVAILABLE
                    and d.spec.max_range_km >= min_range_km
                    and d.spec.max_payload_kg >= payload_kg
                    and d.spec.battery_pct >= 80.0)
            ]

        if not candidates:
            duration = time.perf_counter() - start
            audit_service.log_event(
                "FLEET_NO_DRONE", mission_id,
                {"error": "No drone matches criteria", "duration_sec": round(duration, 3)},
                "FleetManager"
            )
            return FleetSelectionResult(
                success=False, selected_drone=None,
                distance_km=0.0, duration_sec=duration,
                error_msg="No available drone matching criteria"
            )

        # Select drone closest to target (minimizes flight time)
        def score(drone: DroneUnit) -> float:
            depot = DEPOT_COORDS.get(drone.spec.position_depot, (36.0, 2.0))
            dist = haversine_km(*depot, *TARGET_COORDS)
            return dist  # lower is better

        best = min(candidates, key=score)
        depot_coords = DEPOT_COORDS.get(best.spec.position_depot, (36.0, 2.0))
        distance = haversine_km(*depot_coords, *TARGET_COORDS)
        duration = time.perf_counter() - start

        audit_service.log_event(
            "FLEET_DRONE_SELECTED", mission_id,
            {
                "drone_id": best.spec.drone_id,
                "depot": best.spec.position_depot,
                "range_km": best.spec.max_range_km,
                "battery_pct": best.spec.battery_pct,
                "distance_km": round(distance, 1),
                "duration_sec": round(duration, 3)
            },
            "FleetManager"
        )

        return FleetSelectionResult(
            success=True, selected_drone=best,
            distance_km=round(distance, 1), duration_sec=duration
        )

    def assign_mission(self, drone: DroneUnit, mission_id: str) -> bool:
        """Mark drone as in-mission (part of 2-phase commit)."""
        with self._lock:
            if drone.spec.status != DroneStatus.AVAILABLE:
                return False
            drone.spec.status = DroneStatus.IN_MISSION
            audit_service.log_event(
                "DRONE_ASSIGNED", mission_id,
                {"drone_id": drone.spec.drone_id},
                "FleetManager"
            )
            return True

    def release_drone(self, drone: DroneUnit, mission_id: str):
        """Release drone back to available pool (rollback or mission complete)."""
        with self._lock:
            drone.spec.status = DroneStatus.AVAILABLE
            audit_service.log_event(
                "DRONE_RELEASED", mission_id,
                {"drone_id": drone.spec.drone_id},
                "FleetManager"
            )
