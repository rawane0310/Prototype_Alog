"""
EMSDN – Presentation Layer: Clinic Client
Simulates a rural clinic submitting emergency requests via REST API.
Registers a WebSocket callback to receive real-time push notifications.
"""
import time
from colorama import Fore, Style

from business.scheduling_engine import DispatchRequest, SchedulingEngine
from business.notification_service import notification_service, Notification


class ClinicClient:
    """
    Simulates the clinic-side REST client and WebSocket listener.
    In production: HTTP POST to /dispatches, WebSocket on /ws/notifications.
    """

    def __init__(self, clinic_id: str, clinic_name: str, token: str):
        self.clinic_id = clinic_id
        self.clinic_name = clinic_name
        self.token = token
        self._notifications_received: list[Notification] = []

        # Register WebSocket listener (push notifications)
        notification_service.register_websocket(
            clinic_id, self._on_notification
        )

    def submit_emergency_request(self, engine: SchedulingEngine,
                                  supply_type: str, quantity: int,
                                  urgency: str, window_minutes: int) -> str:
        """
        POST /dispatches — Submit emergency supply request.
        Returns mission_id.
        """
        print(f"\n{Fore.YELLOW}{'═'*65}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}  [CLINIC REQUEST] {self.clinic_name}{Style.RESET_ALL}")
        print(f"  Supply: {supply_type} x{quantity} | Urgency: {urgency}")
        print(f"  Biological window: {window_minutes} minutes")
        print(f"{Fore.YELLOW}{'═'*65}{Style.RESET_ALL}\n")

        request = DispatchRequest(
            request_id=f"REQ-{self.clinic_id}-{int(time.time())}",
            clinic_id=self.clinic_id,
            supply_type=supply_type,
            quantity=quantity,
            urgency_level=urgency,
            window_minutes=window_minutes
        )

        mission_id = engine.receive_request(request)
        return mission_id

    def _on_notification(self, notif: Notification):
        """WebSocket push callback – called by NotificationService."""
        self._notifications_received.append(notif)
        severity_color = {
            "critical": Fore.RED,
            "warning": Fore.YELLOW,
            "info": Fore.GREEN
        }.get(notif.severity, Fore.WHITE)

        print(f"\n{severity_color}  [WebSocket → {self.clinic_name}] "
              f"{notif.message}{Style.RESET_ALL}")

    def get_notifications(self) -> list[Notification]:
        return list(self._notifications_received)
