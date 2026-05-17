"""
EMSDN – Infrastructure Layer: Inventory Adapters
Normalizes heterogeneous data from blood banks (HL7 FHIR) and pharmacies (REST/JSON)
into the EMSDN internal format before exposing it to the Inventory Matcher.
"""
import time
import random
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class SupplyType(Enum):
    BLOOD_O_NEG = "O-"
    BLOOD_O_POS = "O+"
    BLOOD_AB_NEG = "AB-"
    ANTIVENOM = "antivenom_viper"
    EPINEPHRINE = "epinephrine_1mg"
    INSULIN = "insulin_rapid"


@dataclass
class SupplyUnit:
    supply_id: str
    supply_type: str
    quantity: int
    location_name: str
    location_lat: float
    location_lon: float
    temp_history_ok: bool       # Cold chain integrity verified
    expiry_days_remaining: int
    source_adapter: str         # "blood_bank_chlef", "pharmacy_tiaret", etc.


class BloodBankAdapter:
    """
    Adapter for blood bank REST/HL7 FHIR interface.
    Simulates 3 connected blood banks with realistic inventory.
    Introduces a configurable latency to simulate real API calls.
    """
    SIMULATED_LATENCY_SEC = 0.4  # Accelerated (real: ~800ms)

    def __init__(self, fail_on_request: bool = False):
        self._fail = fail_on_request
        self._inventory = {
            "blood_bank_chlef": [
                SupplyUnit("BB-C-001", "O-", 4, "Chlef", 36.165, 1.329,
                           True, 12, "blood_bank_chlef"),
                SupplyUnit("BB-C-002", "O+", 6, "Chlef", 36.165, 1.329,
                           True, 8, "blood_bank_chlef"),
                SupplyUnit("BB-C-003", "AB-", 2, "Chlef", 36.165, 1.329,
                           True, 15, "blood_bank_chlef"),
            ],
            "blood_bank_tiaret": [
                SupplyUnit("BB-T-001", "O-", 1, "Tiaret", 35.370, 1.320,
                           True, 3, "blood_bank_tiaret"),
                SupplyUnit("BB-T-002", "O+", 8, "Tiaret", 35.370, 1.320,
                           False, 7, "blood_bank_tiaret"),  # cold chain breach!
            ],
            "blood_bank_blida": [
                SupplyUnit("BB-B-001", "O-", 2, "Blida", 36.470, 2.826,
                           True, 20, "blood_bank_blida"),
            ],
        }

    def find_supply(self, supply_type: str, quantity: int) -> list[SupplyUnit]:
        """
        Normalize and search inventory. Simulates network latency.
        Returns only units that pass cold-chain and expiry checks.
        """
        time.sleep(self.SIMULATED_LATENCY_SEC)
        if self._fail:
            raise ConnectionError("Blood bank API unreachable (timeout)")

        results = []
        for bank_name, units in self._inventory.items():
            for unit in units:
                if (unit.supply_type == supply_type
                        and unit.quantity >= quantity
                        and unit.temp_history_ok
                        and unit.expiry_days_remaining > 1):
                    results.append(unit)
        return results

    def reserve(self, supply_id: str, quantity: int) -> bool:
        """Atomic reservation (part of 2-phase commit)."""
        for units in self._inventory.values():
            for unit in units:
                if unit.supply_id == supply_id and unit.quantity >= quantity:
                    unit.quantity -= quantity
                    return True
        return False

    def rollback(self, supply_id: str, quantity: int):
        """Rollback reservation on transaction failure."""
        for units in self._inventory.values():
            for unit in units:
                if unit.supply_id == supply_id:
                    unit.quantity += quantity
                    return


class PharmacyAdapter:
    """
    Adapter for hospital pharmacy REST/JSON interface.
    """
    SIMULATED_LATENCY_SEC = 0.3

    def __init__(self):
        self._inventory = [
            SupplyUnit("PH-001", "antivenom_viper", 3, "Chlef", 36.165, 1.329,
                       True, 90, "pharmacy_chlef"),
            SupplyUnit("PH-002", "epinephrine_1mg", 20, "Blida", 36.470, 2.826,
                       True, 180, "pharmacy_blida"),
            SupplyUnit("PH-003", "insulin_rapid", 15, "Tiaret", 35.370, 1.320,
                       True, 45, "pharmacy_tiaret"),
        ]

    def find_supply(self, supply_type: str, quantity: int) -> list[SupplyUnit]:
        time.sleep(self.SIMULATED_LATENCY_SEC)
        return [u for u in self._inventory
                if u.supply_type == supply_type
                and u.quantity >= quantity
                and u.temp_history_ok]

    def reserve(self, supply_id: str, quantity: int) -> bool:
        for unit in self._inventory:
            if unit.supply_id == supply_id and unit.quantity >= quantity:
                unit.quantity -= quantity
                return True
        return False
