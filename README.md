# EMSDN – Emergency Medical Supply Drone Network
## Prototype Architectural – 2CS SIL Projet ALOG

Prototype fonctionnel implémentant l'architecture finale décrite au Chapitre 5 du rapport.

---

## Prérequis

```bash
Python 3.10+

python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```

## Lancement

```bash

# Scénario complet (hémorragie + benchmark)
python run_demo.py

# Scénario hémorragique uniquement (pipeline complet)
python run_demo.py scenario

# Benchmark des tactiques (séquentiel vs parallèle)
python run_demo.py benchmark

# Afficher l'architecture globale du système
python run_demo.py arch
```

---

## Structure du projet

```
Prototype_Alog/
├── run_demo.py                        # Point d'entrée principal
├── requirements.txt
│
├── infrastructure/                    # Couche Infrastructure
│   ├── drone_simulator.py             # Drones + broker MQTT simulé
│   └── inventory_adapter.py           # Adaptateurs banques de sang + pharmacies
│
├── business/                          # Couche Métier (SOA)
│   ├── scheduling_engine.py           # ★ Orchestrateur + toutes les tactiques
│   ├── inventory_matcher.py           # Matching inventaire (thread 2a)
│   ├── fleet_manager.py               # Sélection drone (thread 2b)
│   ├── flight_monitor.py              # Surveillance MQTT
│   ├── failsafe_controller.py         # Gestion anomalies
│   ├── auth_service.py                # JWT + RBAC
│   ├── audit_service.py               # Journal persistant
│   └── notification_service.py        # Push WebSocket
│
├── presentation/                      # Couche Présentation
│   └── clinic_client.py               # Client REST + WebSocket clinique
│
└── demo/
    ├── scenario_hemorrhagic.py        # Scénario Aïn Defla (Chapitre 7)
    └── benchmark.py                   # Mesure impact tactiques
```

---

## Tactiques architecturales implémentées

| Tactique | Attribut | Fichier | Ligne clé |
|---|---|---|---|
| Introduce Concurrency | Performance | `scheduling_engine.py` | `_step2_parallel()` |
| Deadline Scheduling | Performance | `scheduling_engine.py` | `_insert_sorted()` |
| Active Redundancy | Disponibilité | `scheduling_engine.py` | `create_redundant_pair()` |
| 2-Phase Commit ACID | Disponibilité | `scheduling_engine.py` | `_step3_atomic_commit()` |
| Exception Handling | Disponibilité | `failsafe_controller.py` | `evaluate_anomaly()` |

---

## Scénario de démonstration (Chapitre 7)

Reproduit exactement le scénario du rapport :
- Clinique Aïn Defla soumet: `POST /dispatches {type: 'O-', qty: 2, urgency: 'hemorrhagic_shock', window: 40min}`
- Scheduling Engine (deadline scheduling) fait passer cette demande devant une demande vaccins soumise 0.5s avant
- Steps 2a (Inventory Matcher) et 2b (Fleet Manager) s'exécutent en **parallèle**
- Drone D-07 (Chlef, 85km autonomie) sélectionné et dispatché
- Télémétrie MQTT surveillée pendant le vol
- Notification WebSocket confirmée à la clinique à la livraison

## Benchmark

Mesure quantitative de l'impact de `Introduce Concurrency` :
- **Séquentiel** : ~750ms (400ms inventaire + 350ms flotte)
- **Parallèle** : ~400ms (max des deux)
- **Gain** : ~350ms par dispatch (~47%)
