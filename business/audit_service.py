"""
EMSDN – Business Layer: Audit Service
Persistent event logging for all pipeline events.
Required by Ministère de la Santé for traceability and audit trails.
Thread-safe append-only journal.
"""
import threading
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class AuditEvent:
    event_id: int
    event_type: str
    timestamp: float
    timestamp_str: str
    mission_id: str
    data: dict
    source_service: str


class AuditService:
    """
    Thread-safe audit log. All pipeline events are recorded here.
    Interface: log_event(type, mission_id, data, source) → event_id
               get_log(mission_id) → list[AuditEvent]
    """
    def __init__(self):
        self._log: list[AuditEvent] = []
        self._counter = 0
        self._lock = threading.Lock()

    def log_event(self, event_type: str, mission_id: str,
                  data: dict, source_service: str = "system") -> int:
        with self._lock:
            self._counter += 1
            event = AuditEvent(
                event_id=self._counter,
                event_type=event_type,
                timestamp=time.time(),
                timestamp_str=time.strftime("%H:%M:%S.") +
                              f"{int((time.time() % 1) * 1000):03d}",
                mission_id=mission_id,
                data=data,
                source_service=source_service
            )
            self._log.append(event)
            return self._counter

    def get_log(self, mission_id: str | None = None) -> list[AuditEvent]:
        with self._lock:
            if mission_id is None:
                return list(self._log)
            return [e for e in self._log if e.mission_id == mission_id]

    def print_log(self, mission_id: str | None = None):
        events = self.get_log(mission_id)
        print(f"\n{'─'*65}")
        print(f"  AUDIT LOG {'(mission: ' + mission_id + ')' if mission_id else '(all)'}")
        print(f"{'─'*65}")
        for e in events:
            print(f"  [{e.timestamp_str}] [{e.source_service:<20}] {e.event_type}")
            for k, v in e.data.items():
                print(f"    {k}: {v}")
        print(f"{'─'*65}\n")


# Singleton instance shared across all services
audit_service = AuditService()
