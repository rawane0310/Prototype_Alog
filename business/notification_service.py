"""
EMSDN – Business Layer: Notification Service
Pushes real-time alerts and confirmations to clinics via WebSocket (simulated).
Avoids polling – clinics receive immediate push notifications.
Interface: notify(recipient_id, message, channel, mission_id)
"""
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional
from colorama import Fore, Style


@dataclass
class Notification:
    notification_id: int
    recipient_id: str
    message: str
    channel: str       # "websocket", "sms_fallback"
    mission_id: str
    timestamp: str
    severity: str      # "info", "warning", "critical"


class NotificationService:
    """
    WebSocket-based push notification service.
    Clinics register a callback (simulating an open WebSocket connection).
    """
    def __init__(self):
        self._subscribers: dict[str, Callable] = {}
        self._history: list[Notification] = []
        self._counter = 0
        self._lock = threading.Lock()

    def register_websocket(self, recipient_id: str, callback: Callable):
        """Called when a clinic opens a WebSocket connection."""
        with self._lock:
            self._subscribers[recipient_id] = callback

    def notify(self, recipient_id: str, message: str,
               mission_id: str, severity: str = "info") -> int:
        with self._lock:
            self._counter += 1
            notif = Notification(
                notification_id=self._counter,
                recipient_id=recipient_id,
                message=message,
                channel="websocket" if recipient_id in self._subscribers else "sms_fallback",
                mission_id=mission_id,
                timestamp=time.strftime("%H:%M:%S"),
                severity=severity
            )
            self._history.append(notif)
            callback = self._subscribers.get(recipient_id)

        # Push outside lock to avoid deadlock
        if callback:
            callback(notif)
        return self._counter

    def get_history(self, recipient_id: Optional[str] = None) -> list[Notification]:
        with self._lock:
            if recipient_id:
                return [n for n in self._history if n.recipient_id == recipient_id]
            return list(self._history)


# Singleton
notification_service = NotificationService()
