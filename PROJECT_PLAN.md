# ICS 2403 — Distributed Computing & Applications
## Distributed 5G/6G Network Service Platform — Project Roadmap

**One system. Twelve evolutionary milestones.** Everything below builds on the
same `common/` codebase and the same 5-node topology (`edge-1, edge-2, ran-1,
core-1, cloud-1`) introduced in Milestone 1. Nothing is rebuilt from scratch.

## Status

| Deliverable | Milestones | Status |
|---|---|---|
| **Assignment 1** | M1 Distributed OS Foundation, M2 Processing & Performance | ✅ **Done** — `assignment1_m1_m2/` |
| **Assignment 2** | M3 Distributed Architecture, M4 Algorithms & Coordination | ✅ **Done** — `assignment2_m3_m4/` |
| **Assignment 3** | M5 Distributed Transactions, M6 Concurrency & Deadlock | ⏳ Not started |
| **Practical 1** | M7 Fault Tolerance & Recovery, M8 Distributed System Models | ⏳ Not started |
| **Practical 2** | M9 Transparency & Reliability, M10 Naming & Process Mgmt | ⏳ Not started |
| **Practical 3** | M11 RPC & Distributed Shared Memory, M12 Distributed File System / Capstone | ⏳ Not started |
| **CAT 1** | Technical report, M1–M12 (30-section structure from the brief) | ⏳ Not started (assemble once all milestones exist) |
| **CAT 2** | Conference-style research paper (derived from CAT 1, kept concise) | ⏳ Not started |
| **CAT 3** | Theoretical concept exam — curated question bank, M1–M12 | ⏳ Not started |

## Repository layout

```
5g6g_platform/
├── common/                        # shared engine — reused by every milestone
│   ├── node.py                    # Node = OS process + resource mgr + process mgr + comms
│   ├── resource_manager.py        # CPU/memory/bandwidth allocation ledger
│   ├── process_manager.py         # process creation, FIFO/priority scheduling
│   ├── communication.py           # TCP + length-prefixed JSON transport
│   ├── topology.py                # the ONE topology every milestone reuses
│   ├── workload_generator.py      # URLLC/eMBB/mMTC synthetic telecom workload
│   └── performance_monitor.py     # throughput/latency/jitter/loss/bottleneck metrics
├── assignment1_m1_m2/             # ✅ done
│   ├── milestone1_foundation.py
│   ├── milestone2_processing_performance.py
│   ├── ASSIGNMENT1_REPORT.md      # deliverables write-up + presentation summary
│   └── results/                   # real JSON output from running the scripts
├── assignment2_m3_m4/             # ✅ done (+ Big Data / Power BI extension)
│   ├── milestone3_architecture.py
│   ├── milestone4_coordination.py
│   ├── assignment2_bigdata_extension.py   # Spark batch layer + Power BI CSV exports
│   ├── POWERBI_DASHBOARD_GUIDE.md         # step-by-step Power BI build guide
│   ├── ASSIGNMENT2_REPORT.md      # deliverables write-up + presentation summary
│   └── results/                   # real JSON/CSV output, incl. results/powerbi/ and results/bigdata/
├── assignment3_m5_m6/             # next
├── assignment3_m5_m6/
├── practical1_m7_m8/
├── practical2_m9_m10/
├── practical3_m11_m12/
├── cat1_technical_report/
├── cat2_research_paper/
└── cat3_theory/
```

## How each future stage will extend the system (so evolution stays continuous)

* **M3 (Architecture):** ✅ done — service decomposition (`SERVICES_BY_TYPE`),
  deployment architecture, scalability + edge-vs-core trade-off experiments.
* **M4 (Coordination):** ✅ done — `common/coordination.py` (Lamport clocks +
  Bully leader election), wired into every node via `node.py`'s existing
  message handling. `election_id` ranking and `current_leader` state are now
  part of every node and will be reused by M5 (transaction coordinator) and
  M7 (automatic re-election on node failure).
* **M5 (Transactions):** wrap multi-node task placement (e.g. an eMBB session
  needing Edge+Core+Cloud coordination) in a 2PC prepare/commit protocol,
  coordinated by the node `current_leader` already elected in M4.
* **M6 (Concurrency/Deadlock):** stress the existing `ResourceManager` with
  concurrent multi-resource requests to induce and then detect/prevent
  deadlock.
* **M7 (Fault tolerance):** kill node OS processes deliberately (they're
  already independent `multiprocessing.Process` instances — this was chosen
  in M1 specifically to make this possible) and measure MTTR/availability.
* **M8 (Models):** re-run the M2 performance harness under host-based /
  processor-pool / server-based configurations of the same topology.
* **M9 (Transparency):** add a location/failure-transparent proxy in front of
  the existing node addresses.
* **M10 (Naming):** replace hardcoded `NODE_ADDR` lookups with a name-server
  node.
* **M11 (RPC/DSM):** formalize `communication.py`'s framing into a proper
  client-stub/server-stub RPC layer; add a shared distributed state store.
* **M12 (Capstone):** add a storage layer (simple distributed KV/file store)
  and run the Final Capstone Experiment (Section 5 of the brief) varying
  nodes/users/bandwidth/failure rate across the whole assembled system.

## Next step

Say "continue with Assignment 2" (or any specific milestone/practical/CAT) and
it will be built the same way: working Python code with `[M#]`-tagged console
output, results saved to disk, and a concise `ASSIGNMENTx_REPORT.md` with a
presentation-ready summary at the end.
