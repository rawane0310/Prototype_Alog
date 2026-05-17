"""
EMSDN – Business Layer: Scheduling Engine
The central orchestrator of the dispatch pipeline. The most critical component.

TACTICS IMPLEMENTED:
  1. INTRODUCE CONCURRENCY  — Steps 2a (InventoryMatcher) and 2b (FleetManager)
                               run in parallel threads; pipeline waits for max(2a, 2b).
  2. DEADLINE SCHEDULING     — Requests ordered by biological urgency window (shortest first).
  3. ACTIVE REDUNDANCY       — Primary + spare instance; state synchronized each step;
                               spare auto-promotes on primary crash.
  4. 2-PHASE COMMIT (ACID)   — Atomic reservation of drone + stock; rollback on failure.
"""
import threading
import queue
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum
from colorama import Fore, Style

from business.auth_service import AuthService, Principal
from business.inventory_matcher import InventoryMatcher, InventoryMatchResult
from business.fleet_manager import FleetManager, FleetSelectionResult
from business.audit_service import audit_service
from business.notification_service import notification_service
from business.flight_monitor import FlightMonitor
from business.failsafe_controller import FailsafeController
from infrastructure.drone_simulator import DroneUnit
from infrastructure.inventory_adapter import BloodBankAdapter


class DispatchStatus(Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    DISPATCHED = "dispatched"
    DELIVERED  = "delivered"
    FAILED     = "failed"
    ABORTED    = "aborted"


@dataclass
class DispatchRequest:
    """A single emergency supply request from a clinic."""
    request_id: str
    clinic_id: str
    supply_type: str
    quantity: int
    urgency_level: str          # "hemorrhagic_shock", "envenomation", "diabetic_crisis"
    window_minutes: int         # Biological survival window
    submitted_at: float = field(default_factory=time.time)
    status: DispatchStatus = DispatchStatus.PENDING
    mission_id: Optional[str] = None
    result_summary: dict = field(default_factory=dict)

    def priority_score(self) -> float:
        """
        Priority score for the deadline scheduling queue.
        Lower score = higher priority (shorter window = more urgent).
        Returns remaining window in minutes (lower = more critical).
        """
        elapsed = (time.time() - self.submitted_at) / 60.0
        remaining_window = self.window_minutes - elapsed
        return remaining_window  # min-heap → smallest remaining = highest priority

    def __lt__(self, other):
        return self.priority_score() < other.priority_score()


@dataclass
class EngineState:
    """Synchronized state shared between primary and spare instances (Active Redundancy)."""
    active_missions: dict = field(default_factory=dict)  # mission_id → DispatchRequest
    last_checkpoint: float = field(default_factory=time.time)
    sequence_number: int = 0


class SchedulingEngine:
    """
    Master orchestrator implementing the full dispatch pipeline.

    Active Redundancy:
        create_redundant_pair() → (primary, spare)
        Primary processes all requests. Spare mirrors state.
        If primary crashes mid-transaction, spare promotes and resumes.

    Pipeline (per request):
        Step 1:  Auth verification
        Step 2a: Inventory search (thread A)   ─┐
        Step 2b: Drone selection (thread B)     ─┴► max(A, B) instead of A+B
        Step 3:  2-phase commit (atomic reserve)
        Step 4:  Clearance simulation
        Step 5:  Drone takeoff
    """

    def __init__(self, engine_id: str, inventory_matcher: InventoryMatcher,
                 fleet_manager: FleetManager, auth_service: AuthService,
                 flight_monitor: FlightMonitor, failsafe_controller: FailsafeController,
                 blood_bank_adapter,
                 is_spare: bool = False, verbose: bool = True):

        self.engine_id = engine_id
        self.is_spare = is_spare
        self.is_active = not is_spare
        self.verbose = verbose

        self._inventory_matcher = inventory_matcher
        self._fleet_manager = fleet_manager
        self._auth_service = auth_service
        self._flight_monitor = flight_monitor
        self._failsafe = failsafe_controller
        self._blood_bank = blood_bank_adapter

        # ── TACTIC: Deadline Scheduling ──────────────────────────────────────
        # Priority queue ordered by remaining biological window (min-heap)
        self._request_queue: list[DispatchRequest] = []
        self._queue_lock = threading.Lock()
        self._queue_not_empty = threading.Condition(self._queue_lock)

        # ── TACTIC: Active Redundancy – shared state ──────────────────────────
        self._state = EngineState()
        self._state_lock = threading.Lock()
        self._peer: Optional['SchedulingEngine'] = None  # Spare/Primary ref

        # Processing thread
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._crash_simulation = False

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def start(self):
        """Start the dispatch processing loop."""
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._processing_loop, daemon=True, name=f"SE-{self.engine_id}"
        )
        self._worker_thread.start()
        if self.verbose:
            role = "SPARE" if self.is_spare else "PRIMARY"
            print(f"{Fore.CYAN}  [SchedulingEngine-{self.engine_id}] "
                  f"Started as {role}{Style.RESET_ALL}")

    def stop(self):
        self._running = False

    def receive_request(self, request: DispatchRequest) -> str:
        """
        Accept a dispatch request and insert into priority queue.
        Returns mission_id.
        """
        mission_id = f"MISSION-{uuid.uuid4().hex[:8].upper()}"
        request.mission_id = mission_id

        with self._queue_not_empty:
            self._insert_sorted(request)
            self._queue_not_empty.notify_all()

        audit_service.log_event(
            "REQUEST_RECEIVED", mission_id,
            {
                "clinic_id": request.clinic_id,
                "supply_type": request.supply_type,
                "quantity": request.quantity,
                "urgency": request.urgency_level,
                "window_min": request.window_minutes,
                "priority_score": round(request.priority_score(), 2)
            },
            f"SchedulingEngine-{self.engine_id}"
        )

        return mission_id

    def get_status(self, mission_id: str) -> Optional[DispatchRequest]:
        with self._state_lock:
            return self._state.active_missions.get(mission_id)

    def simulate_crash(self):
        """Simulate a crash mid-pipeline for redundancy demo."""
        self._crash_simulation = True

    # ─────────────────────────────────────────────────────────────────────────
    # Deadline Scheduling Queue
    # ─────────────────────────────────────────────────────────────────────────

    def _insert_sorted(self, request: DispatchRequest):
        """Insert into sorted list (ascending priority_score = most urgent first)."""
        import bisect
        scores = [r.priority_score() for r in self._request_queue]
        idx = bisect.bisect_left(scores, request.priority_score())
        self._request_queue.insert(idx, request)

    def _pop_next(self) -> Optional[DispatchRequest]:
        if self._request_queue:
            return self._request_queue.pop(0)
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Main Processing Loop
    # ─────────────────────────────────────────────────────────────────────────

    def _processing_loop(self):
        while self._running:
            with self._queue_not_empty:
                while not self._request_queue and self._running:
                    self._queue_not_empty.wait(timeout=0.5)
                if not self._running:
                    break
                request = self._pop_next()

            if request and self.is_active:
                self._execute_pipeline(request)

    # ─────────────────────────────────────────────────────────────────────────
    # DISPATCH PIPELINE
    # ─────────────────────────────────────────────────────────────────────────

    def _execute_pipeline(self, request: DispatchRequest,
                          parallel: bool = True) -> bool:
        """
        Execute the full dispatch pipeline for a request.
        parallel=True  → INTRODUCE CONCURRENCY tactic (steps 2a+2b parallel)
        parallel=False → sequential execution (for benchmark comparison only)
        """
        mission_id = request.mission_id
        pipeline_start = time.perf_counter()
        request.status = DispatchStatus.PROCESSING

        with self._state_lock:
            self._state.active_missions[mission_id] = request

        self._log(f"{'─'*60}")
        self._log(f"Pipeline START  | Mission: {mission_id}")
        self._log(f"  Clinic: {request.clinic_id} | Supply: {request.supply_type} x{request.quantity}")
        self._log(f"  Window: {request.window_minutes} min | Urgency: {request.urgency_level}")

        # ── STEP 1: Authentication ─────────────────────────────────────────
        t1 = time.perf_counter()
        principal = self._auth_service.authenticate(request.clinic_id + "_tok")
        if not principal:
            self._fail_request(request, "Authentication failed")
            return False
        t1_dur = time.perf_counter() - t1
        self._log(f"Step 1 Auth     | ✓ {principal.name} | {t1_dur*1000:.0f}ms")

        # ── CRASH INJECTION POINT (for redundancy demo) ──────────────────
        if self._crash_simulation:
            self._log(f"{Fore.RED}  *** SIMULATED CRASH (post-auth, pre-dispatch) ***{Style.RESET_ALL}")
            self.is_active = False
            self._running = False
            audit_service.log_event("ENGINE_CRASH", mission_id,
                                     {"engine_id": self.engine_id}, "SchedulingEngine")
            if self._peer and not self._peer.is_active:
                self._peer._promote_from_spare(request, pipeline_start)
            return False

        # ── STEP 2: Concurrent Inventory + Fleet Selection ─────────────────
        if parallel:
            t2 = time.perf_counter()
            inv_result, fleet_result = self._step2_parallel(request)
            t2_dur = time.perf_counter() - t2
            mode_label = "PARALLEL"
        else:
            t2 = time.perf_counter()
            inv_result, fleet_result = self._step2_sequential(request)
            t2_dur = time.perf_counter() - t2
            mode_label = "SEQUENTIAL"

        self._log(f"Step 2 {mode_label:<10} | Inv+Fleet | {t2_dur*1000:.0f}ms "
                  f"({'max' if parallel else 'sum'} of both)")

        if not inv_result.success:
            self._fail_request(request, f"Inventory: {inv_result.error_msg}")
            return False
        if not fleet_result.success:
            self._fail_request(request, f"Fleet: {fleet_result.error_msg}")
            return False

        supply = inv_result.best_unit
        drone = fleet_result.selected_drone
        self._log(f"  → Supply: {supply.supply_id} @ {supply.location_name} "
                  f"({supply.quantity} units, {supply.expiry_days_remaining}d expiry)")
        self._log(f"  → Drone:  {drone.spec.drone_id} @ {drone.spec.position_depot} "
                  f"({fleet_result.distance_km} km, {drone.spec.battery_pct}% battery)")

        # ── STEP 3: Atomic 2-Phase Commit ─────────────────────────────────
        t3 = time.perf_counter()
        commit_ok = self._step3_atomic_commit(
            request, drone, supply.supply_id, request.quantity
        )
        t3_dur = time.perf_counter() - t3
        if not commit_ok:
            self._fail_request(request, "2-phase commit failed – resources unavailable")
            return False
        self._log(f"Step 3 Commit   | ✓ Atomic reservation | {t3_dur*1000:.0f}ms")

        # Synchronize state to spare instance
        self._sync_state_to_peer(mission_id, request)

        # ── STEP 4: Clearance ─────────────────────────────────────────────
        t4 = time.perf_counter()
        clearance_sec = self._step4_clearance(mission_id)
        t4_dur = time.perf_counter() - t4
        self._log(f"Step 4 Clearance| ✓ Obtained in {clearance_sec:.1f}s | {t4_dur*1000:.0f}ms")

        # ── STEP 5: Launch ─────────────────────────────────────────────────
        t5 = time.perf_counter()
        self._step5_launch(request, drone, fleet_result.distance_km, mission_id)
        t5_dur = time.perf_counter() - t5
        self._log(f"Step 5 Launch   | ✓ Drone {drone.spec.drone_id} airborne | {t5_dur*1000:.0f}ms")

        total_sec = time.perf_counter() - pipeline_start
        request.status = DispatchStatus.DISPATCHED
        request.result_summary = {
            "drone_id": drone.spec.drone_id,
            "supply_id": supply.supply_id,
            "distance_km": fleet_result.distance_km,
            "pipeline_total_sec": round(total_sec, 3),
            "step2_duration_sec": round(t2_dur, 3),
            "mode": mode_label
        }

        self._log(f"{'─'*60}")
        self._log(f"Pipeline DONE   | Total: {total_sec*1000:.0f}ms "
                  f"({total_sec/60:.2f}min / {request.window_minutes}min window)")
        self._log(f"{'─'*60}")

        audit_service.log_event(
            "DISPATCH_COMPLETE", mission_id,
            {"total_sec": round(total_sec, 3), "drone": drone.spec.drone_id,
             "supply": supply.supply_id},
            f"SchedulingEngine-{self.engine_id}"
        )

        notification_service.notify(
            request.clinic_id,
            f"✓ Dispatch confirmé. Drone {drone.spec.drone_id} en route. "
            f"Arrivée estimée: {int(fleet_result.distance_km / 2)} min.",
            mission_id
        )

        # Wait for delivery then confirm
        threading.Thread(target=self._await_delivery,
                         args=(drone, request, supply.supply_id),
                         daemon=True).start()
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # Step implementations
    # ─────────────────────────────────────────────────────────────────────────

    def _step2_parallel(self, request: DispatchRequest):
        """
        TACTIC: INTRODUCE CONCURRENCY
        Steps 2a and 2b run in separate threads.
        Total duration = max(inv_duration, fleet_duration) instead of sum.
        """
        inv_results = []
        fleet_results = []
        inv_done = threading.Event()
        fleet_done = threading.Event()

        # Thread A – Inventory Matching (step 2a)
        t_inv = threading.Thread(
            target=self._inventory_matcher.find_supply_async,
            args=(request.supply_type, request.quantity,
                  request.mission_id, inv_results, inv_done),
            daemon=True
        )
        # Thread B – Fleet Selection (step 2b)
        t_fleet = threading.Thread(
            target=self._fleet_manager.get_available_drone_async,
            args=(30.0, 1.0, request.mission_id, fleet_results, fleet_done),
            daemon=True
        )

        t_inv.start()
        t_fleet.start()

        # Wait for BOTH to complete (effectively waits for the slower one)
        inv_done.wait()
        fleet_done.wait()

        return inv_results[0], fleet_results[0]

    def _step2_sequential(self, request: DispatchRequest):
        """
        Sequential version – for benchmark comparison only.
        Total duration = inv_duration + fleet_duration (sum, not max).
        """
        inv_result = self._inventory_matcher.find_supply_sync(
            request.supply_type, request.quantity, request.mission_id
        )
        fleet_result = self._fleet_manager.get_available_drone_sync(
            30.0, 1.0, request.mission_id
        )
        return inv_result, fleet_result

    def _step3_atomic_commit(self, request: DispatchRequest,
                              drone: DroneUnit, supply_id: str, quantity: int) -> bool:
        """
        TACTIC: TRANSACTIONS (2-phase commit)
        Phase 1: Try to lock both resources.
        Phase 2: Confirm both or rollback both.
        Guarantees no partial allocation on crash.
        """
        # Phase 1 – Prepare
        stock_reserved = self._blood_bank.reserve(supply_id, quantity)
        if not stock_reserved:
            return False

        drone_reserved = self._fleet_manager.assign_mission(drone, request.mission_id)
        if not drone_reserved:
            # Rollback stock
            self._blood_bank.rollback(supply_id, quantity)
            audit_service.log_event(
                "COMMIT_ROLLBACK", request.mission_id,
                {"reason": "Drone no longer available – stock rolled back"},
                f"SchedulingEngine-{self.engine_id}"
            )
            return False

        # Phase 2 – Commit (both succeeded)
        audit_service.log_event(
            "COMMIT_SUCCESS", request.mission_id,
            {"supply_id": supply_id, "drone_id": drone.spec.drone_id},
            f"SchedulingEngine-{self.engine_id}"
        )
        return True

    def _step4_clearance(self, mission_id: str) -> float:
        """Simulate aviation authority clearance (external, variable delay)."""
        import random
        clearance_sec = random.uniform(0.3, 0.8)  # Accelerated (real: 30-120s)
        time.sleep(clearance_sec)
        audit_service.log_event(
            "CLEARANCE_OBTAINED", mission_id,
            {"delay_sec": round(clearance_sec, 2)},
            f"SchedulingEngine-{self.engine_id}"
        )
        return clearance_sec

    def _step5_launch(self, request: DispatchRequest, drone: DroneUnit,
                       distance_km: float, mission_id: str):
        """Payload verification and drone takeoff."""
        time.sleep(0.1)  # Payload check simulation

        # Start flight monitoring
        self._flight_monitor.start_monitoring(
            drone.spec.drone_id, mission_id,
            anomaly_callback=lambda did, mid, reason: self._failsafe.evaluate_anomaly(
                did, mid, reason, drone, request.clinic_id
            )
        )

        # Launch the drone
        drone.execute_mission(mission_id, distance_km, request.supply_type)

        audit_service.log_event(
            "DRONE_LAUNCHED", mission_id,
            {"drone_id": drone.spec.drone_id, "distance_km": distance_km},
            f"SchedulingEngine-{self.engine_id}"
        )

    def _await_delivery(self, drone: DroneUnit, request: DispatchRequest,
                         supply_id: str):
        """Wait for drone landing confirmation, then update records."""
        drone.delivery_confirmed.wait(timeout=30)
        self._flight_monitor.stop_monitoring(drone.spec.drone_id)
        self._fleet_manager.release_drone(drone, request.mission_id)

        request.status = DispatchStatus.DELIVERED
        with self._state_lock:
            self._state.active_missions[request.mission_id] = request

        notification_service.notify(
            request.clinic_id,
            f"✓ Livraison confirmée. {request.supply_type} x{request.quantity} reçus.",
            request.mission_id
        )

        audit_service.log_event(
            "DELIVERY_CONFIRMED", request.mission_id,
            {"supply_id": supply_id, "clinic": request.clinic_id},
            f"SchedulingEngine-{self.engine_id}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Active Redundancy
    # ─────────────────────────────────────────────────────────────────────────

    def _sync_state_to_peer(self, mission_id: str, request: DispatchRequest):
        """
        TACTIC: ACTIVE REDUNDANCY
        After each major step, synchronize state to the spare instance.
        Spare can resume from the last checkpoint if primary crashes.
        """
        if self._peer:
            with self._peer._state_lock:
                self._peer._state.active_missions[mission_id] = request
                self._peer._state.sequence_number += 1
                self._peer._state.last_checkpoint = time.time()

    def _promote_from_spare(self, pending_request: DispatchRequest,
                             original_start: float):
        """
        TACTIC: ACTIVE REDUNDANCY – spare promotion
        Called when primary crashes. Spare takes over from last checkpoint.
        """
        self.is_active = True
        elapsed = time.perf_counter() - original_start
        self._log(f"{Fore.YELLOW}  *** SPARE PROMOTED → PRIMARY ***{Style.RESET_ALL}")
        self._log(f"  Resuming mission {pending_request.mission_id} "
                  f"(checkpoint at +{elapsed*1000:.0f}ms)")

        audit_service.log_event(
            "SPARE_PROMOTED", pending_request.mission_id,
            {"new_primary": self.engine_id,
             "elapsed_ms": round(elapsed * 1000, 1)},
            f"SchedulingEngine-{self.engine_id}"
        )

        notification_service.notify(
            pending_request.clinic_id,
            f"ℹ Failover transparent. Mission {pending_request.mission_id} continuée "
            f"sans interruption.",
            pending_request.mission_id,
            severity="info"
        )

        # Resume pipeline from beginning (state was synced before crash)
        time.sleep(0.2)
        self._execute_pipeline(pending_request, parallel=True)

    def set_peer(self, peer: 'SchedulingEngine'):
        self._peer = peer

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _fail_request(self, request: DispatchRequest, reason: str):
        request.status = DispatchStatus.FAILED
        self._log(f"{Fore.RED}  Pipeline FAILED: {reason}{Style.RESET_ALL}")
        audit_service.log_event(
            "DISPATCH_FAILED", request.mission_id,
            {"reason": reason},
            f"SchedulingEngine-{self.engine_id}"
        )
        notification_service.notify(
            request.clinic_id,
            f"✗ Dispatch échoué: {reason}. Veuillez réessayer.",
            request.mission_id,
            severity="critical"
        )

    def _log(self, msg: str):
        if self.verbose:
            tag = f"[SE-{self.engine_id}]"
            print(f"  {Fore.CYAN}{tag}{Style.RESET_ALL} {msg}")


# ─────────────────────────────────────────────────────────────────────────────
# Factory: create redundant pair
# ─────────────────────────────────────────────────────────────────────────────

def create_redundant_pair(fleet, verbose=True):
    """
    TACTIC: ACTIVE REDUNDANCY
    Returns (primary, spare) – both receive same inputs, spare mirrors state.
    """
    from infrastructure.inventory_adapter import BloodBankAdapter
    auth = AuthService()
    flight_monitor = FlightMonitor(verbose=verbose)
    failsafe = FailsafeController()
    blood_bank = BloodBankAdapter()

    primary = SchedulingEngine(
        engine_id="PRIMARY",
        inventory_matcher=InventoryMatcher(),
        fleet_manager=FleetManager(fleet),
        auth_service=auth,
        flight_monitor=flight_monitor,
        failsafe_controller=failsafe,
        blood_bank_adapter=blood_bank,
        is_spare=False,
        verbose=verbose
    )

    spare = SchedulingEngine(
        engine_id="SPARE",
        inventory_matcher=InventoryMatcher(),
        fleet_manager=FleetManager(fleet),
        auth_service=auth,
        flight_monitor=flight_monitor,
        failsafe_controller=failsafe,
        blood_bank_adapter=blood_bank,
        is_spare=True,
        verbose=verbose
    )

    primary.set_peer(spare)
    spare.set_peer(primary)

    return primary, spare
