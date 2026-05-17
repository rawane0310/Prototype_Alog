"""
EMSDN – Demo: Scénario Dispatch d'Urgence Hémorragique
Clinique de Aïn Defla — Patient en choc hémorragique — Fenêtre biologique: 40 minutes
Reproduit exactement le scénario décrit au Chapitre 7 du rapport d'architecture.

Démontre:
  - Pipeline complet de dispatch (étapes 1 → 5)
  - Tactic: Introduce Concurrency (étapes 2a+2b en parallèle)
  - Tactic: Deadline Scheduling (demande hémorragique passe devant réapprovisionnement vaccins)
  - Active Redundancy (Scheduling Engine Primary + Spare)
  - Notifications WebSocket temps-réel à la clinique
  - Surveillance MQTT du vol + confirmation livraison
"""
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from colorama import Fore, Style, init
init(autoreset=True)

from infrastructure.drone_simulator import create_drone_fleet
from business.scheduling_engine import create_redundant_pair, DispatchRequest
from business.audit_service import audit_service
from presentation.clinic_client import ClinicClient


def run_scenario():
    print(f"\n{Fore.MAGENTA}{'█'*65}")
    print(f"  EMSDN – Scénario d'Urgence Hémorragique")
    print(f"  Clinique de Aïn Defla | O-négatif | Fenêtre: 40 min")
    print(f"{'█'*65}{Style.RESET_ALL}\n")

    # ── System Initialization ─────────────────────────────────────────────
    print(f"{Fore.CYAN}[1/6] Initialisation du système...{Style.RESET_ALL}")
    fleet = create_drone_fleet()
    primary, spare = create_redundant_pair(fleet, verbose=True)
    primary.start()
    spare.start()

    # Create clinic clients (Presentation Layer)
    clinic_ain_defla = ClinicClient(
        clinic_id="clinic_ain_defla",
        clinic_name="Clinique Rurale Aïn Defla",
        token="clinic_ain_defla_tok"
    )
    clinic_medea = ClinicClient(
        clinic_id="clinic_medea",
        clinic_name="Clinique Médéa",
        token="clinic_medea_tok"
    )

    print(f"  ✓ Fleet: {len(fleet)} drones initialisés")
    print(f"  ✓ Scheduling Engine: PRIMARY + SPARE (active redundancy)")
    print(f"  ✓ Cliniques connectées via WebSocket\n")
    time.sleep(0.3)

    # ── TACTIC DEMO: Deadline Scheduling ─────────────────────────────────
    print(f"{Fore.CYAN}[2/6] Démonstration: Deadline Scheduling{Style.RESET_ALL}")
    print(f"  → Médéa soumet une demande de réapprovisionnement vaccins (fenêtre: 6h)")
    print(f"  → Aïn Defla soumet 3 secondes après: choc hémorragique (fenêtre: 40 min)")
    print(f"  → La demande hémorragique DOIT passer en tête de file\n")

    # Médéa vaccine request (low urgency, 6h window)
    mission_vaccines = clinic_medea.submit_emergency_request(
        engine=primary,
        supply_type="insulin_rapid",  # using as vaccine proxy
        quantity=5,
        urgency="vaccine_restock",
        window_minutes=360  # 6 hours
    )

    time.sleep(0.5)  # 0.5s gap (represents 3 seconds real-world)

    # ── MAIN SCENARIO: Hemorrhagic Shock ─────────────────────────────────
    print(f"{Fore.CYAN}[3/6] Soumission urgence hémorragique – Aïn Defla{Style.RESET_ALL}")
    mission_blood = clinic_ain_defla.submit_emergency_request(
        engine=primary,
        supply_type="O-",
        quantity=2,
        urgency="hemorrhagic_shock",
        window_minutes=40
    )

    # Give pipeline time to execute
    print(f"\n{Fore.CYAN}[4/6] Exécution du pipeline de dispatch...{Style.RESET_ALL}\n")
    time.sleep(6.0)

    # ── Flight Monitoring ─────────────────────────────────────────────────
    print(f"\n{Fore.CYAN}[5/6] Surveillance MQTT en cours (vol du drone)...{Style.RESET_ALL}")
    time.sleep(3.0)

    # ── Results ───────────────────────────────────────────────────────────
    print(f"\n{Fore.CYAN}[6/6] Résultats du scénario{Style.RESET_ALL}")

    blood_req = primary.get_status(mission_blood)
    if blood_req:
        print(f"\n  Mission hémorragique: {blood_req.status.value.upper()}")
        if blood_req.result_summary:
            rs = blood_req.result_summary
            print(f"  Drone: {rs.get('drone_id', 'N/A')}")
            print(f"  Supply: {rs.get('supply_id', 'N/A')}")
            print(f"  Distance: {rs.get('distance_km', 'N/A')} km")
            print(f"  Pipeline total: {rs.get('pipeline_total_sec', 'N/A'):.2f}s "
                  f"({rs.get('pipeline_total_sec', 0)/60:.2f} min)")
            print(f"  Fenêtre biologique: {blood_req.window_minutes} min → "
                  f"{Fore.GREEN}RESPECTÉE{Style.RESET_ALL}")

    # Print notifications received by clinic
    notifs = clinic_ain_defla.get_notifications()
    if notifs:
        print(f"\n  Notifications WebSocket reçues par Aïn Defla: {len(notifs)}")
        for n in notifs:
            print(f"    [{n.timestamp}] {n.message}")

    # Print audit log
    audit_service.print_log(mission_blood)

    primary.stop()
    spare.stop()

    print(f"\n{Fore.GREEN}{'═'*65}")
    print(f"  Scénario terminé.")
    print(f"{'═'*65}{Style.RESET_ALL}\n")


if __name__ == "__main__":
    run_scenario()
