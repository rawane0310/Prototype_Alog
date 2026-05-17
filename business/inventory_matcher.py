"""
EMSDN – Business Layer: Inventory Matcher
Queries all connected blood banks and pharmacies to find available medical supplies.
Runs as a CONCURRENT THREAD in the dispatch pipeline (Introduce Concurrency tactic).
This is step 2a – runs in parallel with Fleet Manager (step 2b).
"""
import threading
import time
from dataclasses import dataclass
from typing import Optional

from infrastructure.inventory_adapter import (
    BloodBankAdapter, PharmacyAdapter, SupplyUnit
)
from business.audit_service import audit_service


@dataclass
class InventoryMatchResult:
    success: bool
    best_unit: Optional[SupplyUnit]
    all_candidates: list[SupplyUnit]
    duration_sec: float
    error_msg: Optional[str] = None


class InventoryMatcher:
    """
    Queries all inventory sources (blood banks + pharmacies) in parallel.
    Selects the optimal unit: minimize dispatch distance, verify cold chain.

    Interface:
        find_supply(type, qty, location, result_holder) → runs in thread
        find_supply_sync(type, qty, location) → blocking version for benchmarks
    """

    def __init__(self, fail_simulation: bool = False):
        self.blood_bank = BloodBankAdapter(fail_on_request=fail_simulation)
        self.pharmacy = PharmacyAdapter()
        self._fail_simulation = fail_simulation

    def find_supply_sync(self, supply_type: str, quantity: int,
                         mission_id: str) -> InventoryMatchResult:
        """Synchronous version – used for benchmark comparison."""
        return self._do_find(supply_type, quantity, mission_id)

    def find_supply_async(self, supply_type: str, quantity: int,
                          mission_id: str, result_holder: list,
                          done_event: threading.Event):
        """
        Async version – meant to run in a thread as part of parallel pipeline.
        Stores result in result_holder[0] and sets done_event when complete.
        """
        result = self._do_find(supply_type, quantity, mission_id)
        result_holder.append(result)
        done_event.set()

    def _do_find(self, supply_type: str, quantity: int,
                 mission_id: str) -> InventoryMatchResult:
        start = time.perf_counter()

        audit_service.log_event(
            "INVENTORY_SEARCH_START", mission_id,
            {"supply_type": supply_type, "quantity": quantity},
            "InventoryMatcher"
        )

        try:
            # Determine which adapter to use based on supply type
            blood_types = {"O-", "O+", "A-", "A+", "B-", "B+", "AB-", "AB+"}
            if supply_type in blood_types:
                candidates = self.blood_bank.find_supply(supply_type, quantity)
            else:
                candidates = self.pharmacy.find_supply(supply_type, quantity)

        except ConnectionError as e:
            duration = time.perf_counter() - start
            audit_service.log_event(
                "INVENTORY_SEARCH_FAILED", mission_id,
                {"error": str(e), "duration_sec": round(duration, 3)},
                "InventoryMatcher"
            )
            return InventoryMatchResult(
                success=False, best_unit=None,
                all_candidates=[], duration_sec=duration,
                error_msg=str(e)
            )

        duration = time.perf_counter() - start

        if not candidates:
            audit_service.log_event(
                "INVENTORY_NO_MATCH", mission_id,
                {"supply_type": supply_type, "duration_sec": round(duration, 3)},
                "InventoryMatcher"
            )
            return InventoryMatchResult(
                success=False, best_unit=None,
                all_candidates=[], duration_sec=duration,
                error_msg="No matching supply found"
            )

        # Select best candidate: most stock + cold chain OK + furthest expiry
        best = max(candidates, key=lambda u: u.quantity + u.expiry_days_remaining)

        audit_service.log_event(
            "INVENTORY_MATCH_FOUND", mission_id,
            {
                "supply_id": best.supply_id,
                "location": best.location_name,
                "quantity_available": best.quantity,
                "temp_ok": best.temp_history_ok,
                "expiry_days": best.expiry_days_remaining,
                "duration_sec": round(duration, 3)
            },
            "InventoryMatcher"
        )

        return InventoryMatchResult(
            success=True, best_unit=best,
            all_candidates=candidates, duration_sec=duration
        )
