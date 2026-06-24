# Building an AI-Powered Logistics Command Center: How I Built It Solo

*A deep-dive into architecting a production-grade, event-driven AI platform from scratch*

***

By **Ayaan Shaheer** | Software Engineer & AI Systems Architect

***

When a mid-sized freight company approached me with a simple problem — *"our ops team still runs on Excel and WhatsApp"* — I knew this was going to be one of those projects that doesn't just solve a problem, it redesigns an entire operational nervous system.

This is the story of how I, as a solo architect and engineer, designed and built a **10-module, AI-augmented, event-driven logistics platform** — from blank document to production-ready system.

***

## The Problem I Was Solving

The client operated a large freight network with hundreds of vehicles, multiple warehouses, and hundreds of thousands of monthly shipments. Yet their ops team was:

- Discovering delayed shipments **hours too late**
- Support agents **manually answering** "Where is my parcel?" calls all day
- Managers receiving operational reports **24 hours after the fact**
- Incidents being **created by hand** with no structured classification or routing

The fix wasn't a dashboard. It was a full-stack, intelligent operations platform.

***

## The Architecture Philosophy

Before writing a single line of code, I made the most important decision of the entire project: **event-driven microservices over a monolith.**

Every state change in the system — a shipment update, a delay prediction, an incident creation — emits a Kafka event. Downstream services consume independently. This single decision gave me:

- **Zero coupling** between ingestion, ML inference, and agent logic
- **Independent scalability** per service based on load
- **A durable audit log** of every system event
- **Extensibility** — adding a new consumer never touches existing code

The entire platform was designed in **10 independently deployable microservices.**

***

## System Design Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                                 │
│          Web Dashboard  │  Mobile App  │  ERP Systems               │
└──────────────┬──────────────────────────────────────────────────────┘
               │ REST / WebSocket
┌──────────────▼──────────────────────────────────────────────────────┐
│                     EDGE LAYER                                      │
│              AWS ALB → API Gateway                                  │
│         (TLS termination, rate limiting, JWT validation)            │
└──────────────┬──────────────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────────┐
│                      API LAYER                                      │
│   FastAPI Shipment Ingestion Service  │  Auth Service (JWT/RBAC)    │
└──────────────┬──────────────────────────────────────────────────────┘
               │ Publishes Events
┌──────────────▼──────────────────────────────────────────────────────┐
│              STREAMING LAYER — Apache Kafka (AWS MSK)               │
│                                                                     │
│  shipment-events (24p) │ prediction-events (12p) │ incident-events  │
│  vehicle-events (24p)  │ alert-events (6p)       │ dlq.* queues     │
│                   Confluent Schema Registry (Avro)                  │
└───┬──────────────┬──────────────────────────┬───────────────────────┘
    │              │                          │
┌───▼───┐    ┌─────▼───────┐          ┌──────▼────────┐
│  ML   │    │  AI Agent   │          │  Qdrant Sync  │
│Service│    │  Service    │          │  Service      │
│XGBoost│    │  LangGraph  │          │  (RAG Indexer)│
└───┬───┘    └─────┬───────┘          └──────┬────────┘
    │ prediction-  │ incident-events          │
    │ events       │                   ┌──────▼────────┐
    │         ┌────▼──────┐            │    Qdrant     │
    │         │  Worker   │            │  Vector DB    │
    │         │  Service  │            │  (Self-hosted)│
    │         │  (Celery) │            └──────┬────────┘
    │         └────┬──────┘                   │
    │         Slack│Email│PagerDuty    ┌──────▼────────┐
    │              │                   │  Copilot RAG  │
    │         ┌────▼──────┐            │  Service      │
    │         │  Reports  │            │  LangChain    │
    │         │  Service  │            │  + Gemini     │
    │         └───────────┘            └───────────────┘
    │
┌───▼───────────────────────────────────────────────────────────────┐
│                     DATA LAYER                                    │
│   PostgreSQL 16 (RDS)    │    Redis 7 (ElastiCache)               │
└───────────────────────────────────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────────┐
│                  OBSERVABILITY LAYER                              │
│   Prometheus + Grafana (5 dashboards)  │  Loki  │  Fluent Bit    │
└───────────────────────────────────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────────┐
│               INFRASTRUCTURE LAYER                                │
│   AWS EKS 1.30 + Karpenter  │  Terraform IaC  │  GitHub Actions  │
└───────────────────────────────────────────────────────────────────┘
```

***

## The 10 Modules I Built

### Module 1 — Shipment Ingestion Service

This is the **entry point for all data** — GPS telemetry, warehouse management systems, and partner integrations all hit this service first.

**Stack:** FastAPI 0.111+, PostgreSQL 16, SQLAlchemy 2.0 (async), Redis 7, aiokafka, Avro serialization

The key engineering challenge here was **idempotency**. With hundreds of vehicles pushing GPS events, retry storms are inevitable. I solved this by storing a SHA256 composite key (`shipment_id + timestamp`) in Redis with a 24-hour TTL. Duplicate events return the original `event_id` immediately — no double inserts, no chaos.

The database schema uses **monthly table partitioning** on `created_at` via `pg_partman`, keeping query performance stable as the append-only event log grows to tens of millions of rows.

***

### Module 2 — Event Streaming (Apache Kafka)

Kafka is the **central nervous system** of the entire platform. I used **AWS MSK** (managed Kafka 3.6) to avoid broker operations overhead.

**Topic design decisions I made:**
- `shipment-events` → 24 partitions, keyed on `shipment_id` (guarantees per-shipment ordering)
- `vehicle-events` → 24 partitions, keyed on `vehicle_id`
- `incident-events` → 30-day retention (regulatory)
- All topics → replication factor 3 across 3 AZs (zero data loss)
- Dead Letter Queues (`dlq.*`) for every topic with a Grafana alert on any lag > 0

**Schema Registry** with Avro handles schema evolution safely across all 10 services.

***

### Module 3 — Delay Prediction Engine (ML Service)

This was the most technically satisfying module. The ML service consumes every `shipment-events` message and produces a **delay probability + estimated delay hours** within 100ms.

**Model:** XGBoost Regressor, trained on a 90-day rolling window, retrained every Sunday at 02:00 IST via a GitHub Actions pipeline.

**Features I engineered:**

| Feature | Source |
|---|---|
| Distance remaining (Haversine) | Live GPS → Destination |
| Vehicle avg delay (7d) | PostgreSQL, cached in Redis |
| Driver on-time rate (30d) | Historical driver table |
| Route avg delay (30d) | Materialized view, daily refresh |
| Time of day / Day of week | Event timestamp |
| Weather severity score (1–5) | OpenWeatherMap API, 30-min cache |
| Warehouse dispatch backlog | Redis counter per warehouse |
| Shipment weight bucket | Shipment metadata |

The model is served **in-process** inside FastAPI. A background thread polls MLflow every 5 minutes and hot-swaps the model if a new Production version exists — zero downtime, zero pod restarts.

***

### Module 4 — AI Incident Management Agent (LangGraph)

When predicted delay > 2 hours, this LangGraph-powered agent fires automatically. It creates a structured incident record, classifies severity, identifies root causes, and generates recommendations — **all in under 10 seconds.**

**The 7-node LangGraph StateGraph:**

```
fetch_context → classify_severity → generate_rca → suggest_action
                                                        ↓
                                   pagerduty_triggered ← notify ← create_incident ← store_memory
```

**Severity tiers:** CRITICAL (>6h), HIGH (3–6h), MEDIUM (1–3h), LOW (<1h)

**LLM Stack:**
- Primary: **Gemini 1.5 Flash** (fast, structured JSON output)
- Fallback: **Groq Llama-3 70B** (sub-500ms, fires when Gemini is rate-limited)
- Last resort: Pure rules-based classification if both LLMs fail

The RCA prompt is tightly structured to output valid JSON only — root causes with confidence scores, ranked recommendations, and estimated resolution time. I kept severity classification **rules-based** intentionally — you don't want an LLM deciding what's CRITICAL in production logistics.

***

### Module 5 — Operational Copilot (RAG)

This is the module I'm most proud of. An ops manager can ask in plain English (or Hindi):

> *"Which warehouse caused the most delays this week?"*

And get a grounded, cited answer in seconds.

**RAG Architecture:**
- **Embeddings:** Google `text-embedding-004` (768-dim)
- **Vector Store:** Qdrant (self-hosted on EKS) — chosen over Pinecone for cost and data sovereignty
- **Hybrid Search:** Qdrant sparse + dense (BM25 + semantic) — critical for exact ID recall like `SHP-123`
- **Re-ranking:** `ms-marco-MiniLM-L-6-v2` cross-encoder on top-8 results
- **Orchestration:** LangChain LCEL
- **Generation:** Gemini 1.5 Flash with 1M-token context window

**Query routing pipeline:**
```
User Query → Query Rewriter (LLM) → Intent Classifier
    ↓
SQL_LOOKUP → Text-to-SQL → PostgreSQL
VECTOR_SEARCH → Qdrant hybrid search
HYBRID → Both, merged
    ↓
Re-ranking → Context Assembly → Gemini generation → Response + Citations
```

**Guardrails I implemented:** prompt injection sanitization, mandatory source citation on every response, PII exclusion (driver names/phones never indexed), and RBAC-gated SQL access.

***

### Module 6 — Automated Executive Reports

Every night at **20:00 IST**, a Celery Beat task fires a full report pipeline:

1. Aggregate daily KPIs from PostgreSQL
2. Feed data to Gemini for a 3-paragraph executive narrative
3. Render to PDF via **WeasyPrint** (no headless Chrome — keeps pods lightweight)
4. Deliver via **SendGrid** email + **Slack Bolt** file upload
5. Archive to **S3** (1-year retention) and index into Qdrant for the copilot

***

### Module 7 — Worker Service (Celery)

All async business logic lives here: alerts, escalations, Qdrant sync, ML feature refresh, and scheduled maintenance.

The **incident escalation state machine** is the heart of this module:

```
open → acknowledged → in_progress → resolved
          ↓ (SLA breach)
       escalated → in_progress
          ↓ (ops unavailable)
       pagerduty_triggered
```

Celery is configured with `task_acks_late=True` — tasks are re-queued on worker crash rather than silently lost.

***

### Module 8 — Monitoring (Prometheus + Grafana)

Every service emits structured JSON logs and Prometheus metrics. I built **5 Grafana dashboards:**

1. Fleet Operations Overview
2. AI System Health (ML latency, agent success rate, LLM error rate)
3. Infrastructure Health (Kafka lag, Redis memory, PG connections)
4. Incident Command Center (MTTR trend, open incidents by severity)
5. Business KPIs (SLA compliance, warehouse performance league table)

**Key alert thresholds:** API P99 > 500ms → page, Kafka consumer lag > 1000 for 10 min → alert, LLM API errors > 5/min → auto-switch to fallback.

***

### Module 9 — Cloud Deployment (AWS EKS + Terraform)

The entire infrastructure is **code-first** with Terraform 1.8:

- **EKS 1.30** with 3 node groups: `sys` (monitoring), `app` (API/workers), `ml` (GPU-ready)
- **Karpenter** for autoscaling — scales to zero when idle, spins up in seconds under load
- **ECR** with immutable image tags and Trivy vulnerability scanning on every push
- **Separate RDS and ElastiCache** per environment (dev/staging/prod)

Every service has resource requests/limits, liveness + readiness probes, and `PodDisruptionBudgets` to guarantee availability during rolling updates.

***

### Module 10 — Security

Security was designed in, not bolted on:

- **Authentication:** JWT RS256 (asymmetric keys), validated at the API Gateway layer
- **Authorization:** Role-based (ops_manager, support_agent, executive, system)
- **Secrets:** AWS Secrets Manager — zero secrets in code or environment files
- **Container scanning:** Trivy on every ECR push, Bandit for Python static analysis, Safety for dependency CVEs
- **Network:** Calico CNI for Kubernetes network policies, zero-trust pod-to-pod communication
- **Compliance:** OWASP Top-10 adherent throughout

***

## Full Tech Stack

| Layer | Technology |
|---|---|
| API Framework | FastAPI 0.111+ |
| Language | Python 3.12 |
| Primary Database | PostgreSQL 16 (AWS RDS) |
| Cache | Redis 7 (AWS ElastiCache) |
| Message Broker | Apache Kafka 3.6 (AWS MSK) |
| Schema Registry | Confluent Schema Registry (Avro) |
| ML Model | XGBoost + MLflow Model Registry |
| Agent Framework | LangGraph |
| RAG Orchestration | LangChain LCEL |
| Vector Database | Qdrant (self-hosted on EKS) |
| LLM (Primary) | Google Gemini 1.5 Flash |
| LLM (Fallback) | Groq Llama-3 70B |
| Embeddings | Google text-embedding-004 |
| Task Queue | Celery + Celery Beat |
| Monitoring | Prometheus + Grafana + Loki |
| Log Shipping | Fluent Bit DaemonSet |
| Container Registry | AWS ECR |
| Orchestration | AWS EKS 1.30 + Karpenter |
| IaC | Terraform 1.8 |
| CI/CD | GitHub Actions |
| PDF Rendering | WeasyPrint |
| Email | SendGrid |
| Alerting | Slack Bolt + PagerDuty |
| Security Scanning | Trivy + Bandit + Safety |

***

## What I Learned Building This Solo

**1. Event-driven architecture pays dividends early.** The decision to make Kafka the backbone meant I could build and test each module independently, deploy them separately, and add features without touching existing services.

**2. LangGraph is worth the learning curve.** The graph-based workflow made the incident agent debuggable, extensible, and testable in ways that raw LLM chains simply aren't.

**3. Hybrid RAG search is non-negotiable for domain data.** Pure semantic search fails on exact identifiers like shipment IDs. BM25 + dense vector search together solved this.

**4. Design for failure first.** DLQs, circuit breakers, LLM fallbacks, idempotency keys, state machines — every component assumes something will break. That's what makes it production-grade.

**5. Observability is a feature, not an afterthought.** I wired Prometheus metrics and structured logs from day one. By the time I was debugging Kafka consumer lag at 2am, I was very glad I did.

***

*Building production-grade AI systems is as much about architecture discipline as it is about picking the right model. The model is 10% of the work. The other 90% is everything around it.*

*— Ayaan Shaheer*

***

*If you're building something similar or want to discuss the architecture, find me on [LinkedIn]((https://www.linkedin.com/in/ayaan-shaheer-mlops/)) or [GitHub]((https://github.com/AyaanShaheer)).*
