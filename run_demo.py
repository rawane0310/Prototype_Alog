"""
EMSDN – Prototype Architectural
Point d'entrée principal.

Usage:
    python run_demo.py           # Scénario complet + benchmark
    python run_demo.py scenario  # Scénario hémorragique uniquement
    python run_demo.py benchmark # Benchmark tactiques uniquement
    python run_demo.py arch      # Afficher l'architecture du système
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from colorama import Fore, Style, init
init(autoreset=True)


def print_architecture():
    """Print system architecture overview."""
    print(f"""
{Fore.MAGENTA}{'█'*65}
  EMSDN – Emergency Medical Supply Drone Network
  Architecture Overview
{'█'*65}{Style.RESET_ALL}

{Fore.CYAN}┌──────────────────────────────────────────────────────────────┐
│  COUCHE PRÉSENTATION (Client-Serveur)                        │
│  ClinicClient (REST/WebSocket) │ MinistryDashboard (REST)    │
└──────────────────────────────────────────────────────────────┘
                    ▲ REST API / WebSocket{Style.RESET_ALL}
{Fore.GREEN}┌──────────────────────────────────────────────────────────────┐
│  COUCHE MÉTIER (SOA – Architecture Orientée Services)        │
│                                                              │
│  ┌─────────────────────────────────────┐                    │
│  │ SCHEDULING ENGINE (x2 redondant)    │ ← ORCHESTRATEUR    │
│  │  PRIMARY ←──sync──► SPARE          │   Master-Slave      │
│  │  • File priorité (deadline sched.) │                    │
│  │  • 2-phase commit (ACID)           │                    │
│  └────────┬────────────────┬──────────┘                    │
│           │ PARALLÈLE      │                                │
│  ┌────────▼──────┐ ┌───────▼───────┐                       │
│  │InventoryMatch │ │ FleetManager  │  ← Introduce          │
│  │ (Thread 2a)   │ │ (Thread 2b)   │    Concurrency        │
│  └───────────────┘ └───────────────┘                       │
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────┐ │
│  │ FlightMonitor│  │FailsafeCtrl    │  │ NotificationSvc  │ │
│  │ (MQTT sub.)  │  │ (cross-layer!) │  │ (WebSocket push) │ │
│  └──────────────┘  └────────────────┘  └─────────────────┘ │
│  ┌──────────────┐  ┌────────────────┐                      │
│  │ AuthService  │  │  AuditService  │                      │
│  │ (JWT/RBAC)   │  │  (persistant)  │                      │
│  └──────────────┘  └────────────────┘                      │
└──────────────────────────────────────────────────────────────┘
                    ▲ MQTT / REST{Style.RESET_ALL}
{Fore.YELLOW}┌──────────────────────────────────────────────────────────────┐
│  COUCHE INFRASTRUCTURE PHYSIQUE                              │
│  DroneFleet (MQTT telemetry) │ BloodBankAdapter │ PharmaAdap│
└──────────────────────────────────────────────────────────────┘{Style.RESET_ALL}

{Fore.CYAN}TACTIQUES ARCHITECTURALES IMPLÉMENTÉES:{Style.RESET_ALL}
  1. {Fore.GREEN}Introduce Concurrency{Style.RESET_ALL}   → business/scheduling_engine.py:_step2_parallel()
     Threads 2a (InventoryMatcher) ‖ 2b (FleetManager)
     Gain: max(400ms, 350ms) = 400ms  vs  400+350 = 750ms séquentiel

  2. {Fore.GREEN}Deadline Scheduling{Style.RESET_ALL}     → business/scheduling_engine.py:_insert_sorted()
     File priorité par fenêtre biologique restante
     Choc hémorragique (40min) > Réappro vaccins (6h)

  3. {Fore.GREEN}Active Redundancy{Style.RESET_ALL}       → business/scheduling_engine.py:create_redundant_pair()
     PRIMARY + SPARE synchronisés; failover transparent

  4. {Fore.GREEN}Transactions ACID{Style.RESET_ALL}       → business/scheduling_engine.py:_step3_atomic_commit()
     2-phase commit; rollback si crash mid-transaction

  5. {Fore.GREEN}Exception Handling{Style.RESET_ALL}     → business/inventory_matcher.py + failsafe_controller.py
     Degraded mode si inventaire inaccessible; abort si chaîne du froid compromise

{Fore.CYAN}PROTOCOLES DE COMMUNICATION:{Style.RESET_ALL}
  Drones → FlightMonitor    : MQTT publish-subscribe (fire & forget)
  Cliniques → Scheduling    : REST POST /dispatches (stateless)
  NotificationService → CLI : WebSocket push (temps-réel)
  BloodBank → InvMatcher    : REST/JSON (adaptateur normalisé)
""")


def main():
    args = sys.argv[1:]
    mode = args[0] if args else "all"

    if mode == "arch":
        print_architecture()
        return

    if mode in ("all", "scenario"):
        from demo.scenario_hemorrhagic import run_scenario
        run_scenario()

    if mode in ("all", "benchmark"):
        from demo.benchmark import run_benchmark
        run_benchmark()

    if mode == "all":
        print(f"\n{Fore.CYAN}Pour revoir l'architecture:{Style.RESET_ALL}")
        print(f"  python run_demo.py arch\n")


if __name__ == "__main__":
    main()
