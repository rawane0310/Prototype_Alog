"""
EMSDN – Demo: Benchmark des Tactiques Architecturales
Mesure l'impact RÉEL de la tactique "Introduce Concurrency" sur la latence du pipeline.

Compare:
  MODE SÉQUENTIEL : Inventory Matching → Fleet Selection (sum des durées)
  MODE PARALLÈLE  : Inventory Matching ‖ Fleet Selection (max des durées)

Runs N iterations of each mode and reports:
  - Mean latency (step 2 only)
  - Time saved per dispatch
  - % improvement
  - Statistical variance

Also verifies Deadline Scheduling by submitting mixed-urgency requests.
"""
import time
import sys
import os
import statistics
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colorama import Fore, Style, init
init(autoreset=True)

from infrastructure.drone_simulator import create_drone_fleet, DroneStatus
from infrastructure.inventory_adapter import BloodBankAdapter
from business.inventory_matcher import InventoryMatcher
from business.fleet_manager import FleetManager
from business.auth_service import AuthService
from business.flight_monitor import FlightMonitor
from business.failsafe_controller import FailsafeController
from business.scheduling_engine import (
    SchedulingEngine, DispatchRequest, create_redundant_pair
)
from business.audit_service import audit_service


def measure_step2(parallel: bool, n_runs: int = 5) -> list[float]:
    """
    Directly benchmark step 2 (inventory + fleet) in sequential or parallel mode.
    Returns list of durations in seconds.
    """
    durations = []
    fleet = create_drone_fleet()
    inv_matcher = InventoryMatcher()
    fleet_mgr = FleetManager(fleet)

    # Create a minimal engine just for step2 isolation
    engine = SchedulingEngine(
        engine_id="BENCH",
        inventory_matcher=inv_matcher,
        fleet_manager=fleet_mgr,
        auth_service=AuthService(),
        flight_monitor=FlightMonitor(verbose=False),
        failsafe_controller=FailsafeController(),
        blood_bank_adapter=BloodBankAdapter(),
        verbose=False
    )

    for i in range(n_runs):
        # Reset drone statuses
        for d in fleet:
            d.spec.status = DroneStatus.AVAILABLE

        fake_request = DispatchRequest(
            request_id=f"BENCH-{i}",
            clinic_id="clinic_ain_defla",
            supply_type="O-",
            quantity=2,
            urgency_level="hemorrhagic_shock",
            window_minutes=40,
            mission_id=f"BENCH-MISSION-{i}"
        )

        t0 = time.perf_counter()
        if parallel:
            engine._step2_parallel(fake_request)
        else:
            engine._step2_sequential(fake_request)
        durations.append(time.perf_counter() - t0)

    return durations


def benchmark_deadline_scheduling():
    """
    Verifies that deadline scheduling reorders requests by urgency.
    Submits 3 requests in this order:
      1. Vaccine restock     (window: 360 min)  – arrives first
      2. Diabetic crisis     (window: 90 min)   – arrives second
      3. Hemorrhagic shock   (window: 40 min)   – arrives third
    Expected processing order: 3 → 2 → 1
    """
    print(f"\n{Fore.CYAN}{'─'*60}")
    print(f"  BENCHMARK: Deadline Scheduling")
    print(f"{'─'*60}{Style.RESET_ALL}")

    requests = [
        DispatchRequest("R1", "clinic_medea", "insulin_rapid", 5,
                        "vaccine_restock", 360),
        DispatchRequest("R2", "clinic_ain_defla", "antivenom_viper", 2,
                        "envenomation", 90),
        DispatchRequest("R3", "clinic_ain_defla", "O-", 2,
                        "hemorrhagic_shock", 40),
    ]

    fleet = create_drone_fleet()
    primary, _ = create_redundant_pair(fleet, verbose=False)

    queue_before = []
    for r in requests:
        r.mission_id = f"SCHED-{r.request_id}"
        primary._insert_sorted(r)
        queue_before.append(r.request_id)

    print(f"  Ordre de soumission: {' → '.join(queue_before)}")
    print(f"  Ordre dans la file (par urgence décroissante):")

    for i, r in enumerate(primary._request_queue):
        window_remaining = r.window_minutes
        marker = " ◄ PRIORITÉ MAX" if i == 0 else ""
        print(f"    [{i+1}] {r.request_id} | {r.urgency_level:<20} | "
              f"window: {window_remaining} min{marker}")

    most_urgent = primary._request_queue[0]
    assert most_urgent.urgency_level == "hemorrhagic_shock", \
        "FAIL: hemorrhagic_shock should be first!"

    print(f"\n  {Fore.GREEN}✓ Vérifié: demande hémorragique en tête de file "
          f"(window: {most_urgent.window_minutes} min){Style.RESET_ALL}")
    primary.stop()


def run_benchmark():
    N_RUNS = 6

    print(f"\n{Fore.MAGENTA}{'█'*65}")
    print(f"  EMSDN – Benchmark: Impact de la Tactique 'Introduce Concurrency'")
    print(f"{'█'*65}{Style.RESET_ALL}")
    print(f"\n  {N_RUNS} itérations par mode. Step 2 uniquement (Inv.Matcher + FleetMgr)")
    print(f"  BloodBankAdapter latency: ~400ms | FleetManager latency: ~350ms\n")

    # Sequential
    print(f"{Fore.YELLOW}  Mode SÉQUENTIEL (Inventory THEN Fleet)...{Style.RESET_ALL}")
    seq_times = measure_step2(parallel=False, n_runs=N_RUNS)
    seq_mean = statistics.mean(seq_times)
    seq_std  = statistics.stdev(seq_times) if len(seq_times) > 1 else 0

    # Parallel
    print(f"{Fore.GREEN}  Mode PARALLÈLE (Inventory ‖ Fleet)...{Style.RESET_ALL}")
    par_times = measure_step2(parallel=True, n_runs=N_RUNS)
    par_mean = statistics.mean(par_times)
    par_std  = statistics.stdev(par_times) if len(par_times) > 1 else 0

    saved_ms = (seq_mean - par_mean) * 1000
    pct_gain = (1 - par_mean / seq_mean) * 100 if seq_mean > 0 else 0

    # Results table
    print(f"\n{'─'*65}")
    print(f"  {'Métrique':<35} {'Séquentiel':>12} {'Parallèle':>12}")
    print(f"{'─'*65}")
    print(f"  {'Latence moyenne (ms)':<35} {seq_mean*1000:>10.0f}ms {par_mean*1000:>10.0f}ms")
    print(f"  {'Écart-type (ms)':<35} {seq_std*1000:>10.0f}ms {par_std*1000:>10.0f}ms")
    print(f"  {'Latence min (ms)':<35} {min(seq_times)*1000:>10.0f}ms {min(par_times)*1000:>10.0f}ms")
    print(f"  {'Latence max (ms)':<35} {max(seq_times)*1000:>10.0f}ms {max(par_times)*1000:>10.0f}ms")
    print(f"{'─'*65}")
    print(f"  {Fore.GREEN}Économie de latence (step 2): {saved_ms:+.0f}ms ({pct_gain:.1f}% gain){Style.RESET_ALL}")

    # Theoretical analysis
    inv_lat  = 400  # ms
    fleet_lat = 350  # ms
    seq_theory = inv_lat + fleet_lat
    par_theory = max(inv_lat, fleet_lat)
    print(f"\n  Analyse théorique:")
    print(f"    Séquentiel = {inv_lat}ms + {fleet_lat}ms = {seq_theory}ms")
    print(f"    Parallèle  = max({inv_lat}ms, {fleet_lat}ms) = {par_theory}ms")
    print(f"    Gain théorique: {seq_theory - par_theory}ms ({(1-par_theory/seq_theory)*100:.1f}%)")

    if pct_gain > 20:
        print(f"\n  {Fore.GREEN}✓ TACTIQUE VALIDÉE: La concurrence réduit significativement "
              f"la latence step 2{Style.RESET_ALL}")
    else:
        print(f"\n  {Fore.YELLOW}⚠ Gain mesuré inférieur au théorique "
              f"(overhead threading = {abs(saved_ms - (seq_theory - par_theory)):.0f}ms){Style.RESET_ALL}")

    print(f"\n  Impact sur la fenêtre biologique du patient:")
    print(f"    Window totale: 40 min = 2400 sec")
    print(f"    Économie step 2: ~{par_theory}ms saved (={par_theory/2400/10:.4f}% of window)")
    print(f"    Dans le contexte EMSDN: chaque ms compte biologiquement.")

    # Deadline scheduling demo
    benchmark_deadline_scheduling()

    print(f"\n{Fore.GREEN}{'═'*65}")
    print(f"  Benchmark terminé.")
    print(f"{'═'*65}{Style.RESET_ALL}\n")


if __name__ == "__main__":
    run_benchmark()
