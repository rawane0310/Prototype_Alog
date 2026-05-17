"""
EMSDN – Business Layer: Auth Service
Authenticates clinics and system actors via JWT tokens.
Implements RBAC (Role-Based Access Control).
Simulated for prototype – no real crypto dependencies.
"""
import time
import hashlib
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class Role(Enum):
    CLINIC = "clinic"
    OPERATOR = "operator"
    MINISTRY = "ministry"
    SYSTEM = "system"


@dataclass
class Principal:
    entity_id: str
    name: str
    role: Role
    token: str


# Simulated token registry (in production: JWT verify with secret key)
_TOKEN_DB = {
    "clinic_ain_defla_tok": Principal("C-001", "Clinique Aïn Defla", Role.CLINIC,
                                       "clinic_ain_defla_tok"),
    "clinic_medea_tok":     Principal("C-002", "Clinique Médéa", Role.CLINIC,
                                       "clinic_medea_tok"),
    "system_internal":      Principal("SYS", "EMSDN Internal", Role.SYSTEM,
                                       "system_internal"),
}


class AuthService:
    """
    Verifies JWT tokens and authorizes actions by role.
    Interface: authenticate(token) → Principal | None
               authorize(role, action) → bool
    """
    # Actions allowed per role
    _PERMISSIONS = {
        Role.CLINIC:   {"submit_request", "get_dispatch_status"},
        Role.OPERATOR: {"submit_request", "get_dispatch_status",
                        "manage_drones", "view_fleet"},
        Role.MINISTRY: {"view_audit_log", "view_fleet", "get_dispatch_status"},
        Role.SYSTEM:   {"*"},  # all permissions
    }

    def authenticate(self, token: str) -> Optional[Principal]:
        """Verify token, return Principal or None. Simulates ~50ms validation."""
        time.sleep(0.05)
        return _TOKEN_DB.get(token)

    def authorize(self, principal: Principal, action: str) -> bool:
        perms = self._PERMISSIONS.get(principal.role, set())
        return "*" in perms or action in perms
