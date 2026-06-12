# Cloud ↔ Edge Proxy — Design & Build Plan

> A **domain-agnostic, durable connector** that bridges any **Cloud** application and any **Edge** target (an on-prem device, machine, or network), in both directions, with **Temporal Cloud** as the durable backbone.
>
> The warehouse case (a cloud WMS ↔ on-prem MHE) is used throughout as the **reference example / demo profile** — it is _not_ baked into the core. Swap the profile and the same proxy connects anything cloud-side to anything on-prem.

---

## How to use this document

This plan is split into **three parts**:

- **Part 1 — Proxy Application (Java + Spring Boot + Maven).** The agnostic connector itself, plus a minimal `dummy-cloud` / `dummy-edge` harness to demo it end-to-end. ✅ **Complete.**
- **Part 2 — Management UI (Next.js).** Standalone web app for lifecycle control (start/stop/restart), guided route configuration, and live Temporal visibility. **Not started — fully specified below.**
- **Part 3 — Hardening & rollout.** Additional transports/codecs, profile library, observability, and on-prem packaging. **Not started.**

> ### ⚠️ Implementation conventions (read before coding)
>
> - **Use the `temporal-developer` skill** for every Temporal-touching task — worker/client setup, workflow & activity authoring, standalone-activity APIs, signals/queries, CLI usage, and debugging non-determinism. The Temporal Java APIs below are described conceptually; **confirm exact current signatures via the skill**, especially **Standalone Activities (Public Preview)**, whose API is still stabilizing.
> - For anything else that you need clarification on, please use the **Temporal Docs connector** to ask specific questions.
> - **Language/stack:** Temporal **Java SDK**. **Spring Boot 3.x** application built with **Maven** (`pom.xml`).
> - **No Docker.** Local development uses the **Temporal CLI dev server** (`temporal server start-dev`) and runs apps via `mvn spring-boot:run` / `java -jar`. A **`justfile`** drives build and demo steps.
> - **Stay agnostic.** Core code refers to **Cloud** and **Edge**, never "WMS"/"MHE". Anything domain-specific (message-type names, endpoints, payload formats) lives in a **profile** (configuration), not in core classes.

---

## 1. Context & terminology

The proxy moves **typed messages** between a cloud application and an on-prem target, in both directions:

- **Cloud → Edge** (outbound): the cloud app has something for the on-prem target (a command, task, document, payload).
- **Edge → Cloud** (inbound): the on-prem target has something for the cloud (a confirmation, event, reading, payload).

| Term             | Meaning                                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Cloud**        | Any cloud application using the proxy (the _reference example_ is a Warehouse Management System).                                                        |
| **Edge**         | Any on-prem device, machine, or network endpoint (reference example: Material Handling Equipment).                                                       |
| **Message type** | A named, directional flow defined by configuration (e.g. in the warehouse profile: `WAVE_RELEASE`, `PICK_CONFIRM`). The core treats it as an opaque key. |
| **Profile**      | A bundle of config (message catalog + codecs + endpoints + device templates) for a given domain. The **Warehouse profile** ships as the reference/demo.  |

**Warehouse reference profile (example only):**

| Message type                | Direction    |
| --------------------------- | ------------ |
| `CONTAINER_PUTAWAY`         | Cloud → Edge |
| `PUTAWAY_CONFIRM`           | Edge → Cloud |
| `WAVE_RELEASE` (pick tasks) | Cloud → Edge |
| `PICK_CONFIRM`              | Edge → Cloud |
| `CYCLE_COUNT_REQ`           | Cloud → Edge |
| `CYCLE_COUNT_CONFIRM`       | Edge → Cloud |

The goal is a **universal, multi-tenant** connector that an operator installs on-prem and that "just works," is **remotely controllable** by the cloud-app operator, and is **reconfigurable without redeploys**.

---

## 2. Architecture (applies to all parts)

### Roles

- **Proxy (on-prem)** — the _only_ Temporal **worker**. Connects to Temporal Cloud **egress-only** over the SDK's persistent gRPC channel. **No inbound firewall ports.**
- **Cloud app** — a Temporal **client** (dispatches outbound work and drives the control plane) plus its existing **REST API** (receives inbound messages). Runs **no Temporal worker**.
- **Edge target (on-prem)** — black box. The proxy reaches it on the LAN via pluggable connectors (outbound) and hosts ingress listeners for its pushes (inbound).

### Why this shape

Everything the customer-side box does is **outbound**: proxy → Temporal Cloud (gRPC) and proxy → Cloud REST. The customer never opens an inbound port. Credentials on the customer box are scoped to **their** namespace only, so extraction by the customer exposes only their own data.

### Flows

**Cloud → Edge (outbound).** The cloud client starts a `DeliverToEdge` **workflow** (Workflow ID = `{messageType}-{businessId}`, dedup policy `REJECT_DUPLICATE` + `USE_EXISTING`) on the install's task queue. The proxy worker polls over egress gRPC and runs the workflow, which executes a `TransmitToDevice` activity: look up the route for `(type, CLOUD_TO_EDGE)` → select a `Connector` (HTTP/TCP/FTP) + `MessageCodec` → encode → send to the edge target's channel for that type. Using a workflow gives full visibility in the Temporal UI — each outbound message appears with its activity in the event history.

> **Durability win:** the task lands in Temporal Cloud first, so if the proxy is offline the work waits and delivers on reconnect.

**Edge → Cloud (inbound).** The edge target pushes to a proxy ingress **channel**. The proxy identifies the message type **from the channel, never from the payload** (see §3), then starts a standalone `DeliverToCloud` activity (Activity ID = `{messageType}-{businessId}`, same dedup). The proxy worker runs it, POSTing to that type's cloud endpoint with retries.

> **Reliability levers (cheap, kept):**
>
> - **Ack-after-enqueue** — ack the edge target only _after_ Temporal accepts the activity start, so a retrying device behaves correctly.
> - **Per-transport reliability is documented** — FTP is inherently store-and-forward; raw TCP/HTTP push leans on device retry.
>
> **Out of scope:** no local durable spool. The SDK's auto-reconnecting gRPC channel + Temporal Cloud's 99.9% SLA cover transient cloud-unreachability; the only residual risk (proxy host down) is an HA/shared-responsibility concern a spool can't fix.

### Key design principles

- **Cloud → Edge uses a `DeliverToEdge` workflow** so each outbound dispatch is a first-class Workflow visible in the Temporal UI, with a `TransmitToDevice` activity inside for the actual connector send. **Edge → Cloud uses standalone activities** (`DeliverToCloud`) since that path is a simple fire-and-forget POST — no orchestration needed. The **control plane** (§4) is a long-lived singleton workflow.
- **Workflow/Activity ID = `{messageType}-{businessId}`** is the dedup handle both directions — collapses duplicate cloud starts (workflow ID reuse) and duplicate edge pushes (activity ID reuse) to one execution.
- **Namespace per proxy install** (not task-queue-per-tenant). Per-namespace creds contain the blast radius to that customer's own namespace. The cloud app (trusted/central) holds creds for all namespaces. **Provisioning the customer→namespace+creds mapping lives on the cloud side — out of scope for this repo.**

---

## 3. Channel-based routing (the core of configurability)

**Principle: discriminate by channel, never by payload.** Edge payloads are often standard/opaque and don't self-identify, so the _channel a message uses_ carries the type — symmetrically both directions. Each transport expresses "channel" differently:

| Transport | Inbound channel (→ type)                    | Outbound target (per type)                            |
| --------- | ------------------------------------------- | ----------------------------------------------------- |
| **TCP**   | listen **port**                             | `host:port`                                           |
| **HTTP**  | **path** (e.g. `/pick-confirm`)             | **URL** (e.g. `{baseUrl}/pick-tasks`, `/cycle-count`) |
| **FTP**   | watch **folder** (+ optional filename glob) | remote **folder/path**                                |

- **One inbound channel carries exactly one message type** → the proxy maps inbound channel → type → codec → cloud endpoint with zero payload inspection. Requires the edge target to be pointed at a distinct path/folder/port per type (true for most devices).
- **Symmetric outbound:** the type is already known (it's a known outbound activity); the proxy just needs that type's target channel.
- **Fallback for opaque-multiplexed devices:** an opt-in `MessageTypeResolver` (filename regex / fixed header offset / small content rule) bound to a single channel that carries multiple types. **Off by default** — ship the SPI + a filename-pattern impl, don't force it on anyone.

### Three-layer config (split by who owns it)

This is what keeps configuration **non-technical** — day-to-day ops only touch layer 3.

1. **Cloud message catalog** — shipped/managed by the cloud-app operator (this is the _profile_); the customer never edits. Defines each message type's _direction_, _default codec_, and _cloud endpoint_. So `PICK_CONFIRM` already knows it's inbound and posts to `/api/pick-confirm`. **Customers never type a URL.**
2. **Site infrastructure** — set once at install by IT. Edge addresses/`baseUrl`, certs, and the **available TCP port pool** (e.g. inbound 6000–6010).
3. **Routing bindings** — the only thing ops edits (via the Part 2 UI). For each message type the site uses: pick edge target + transport, and assign the channel (port/path/folder) via a **guided picker** or pre-filled by a **device template**. Validated before apply (in-pool, no channel collisions).

**Device templates** are first-class: a per-edge-model profile that pre-fills the typical message types and channel layout (standard paths, relative port offsets, folder names). The configurator clones it and sets only `baseUrl`/host + base port — in the common case, nothing is typed by hand.

---

## 4. Control plane — remote on/off + hot config (the one real workflow)

Because the proxy is egress-only, the cloud UI can't reach it directly — so control flows _through Temporal Cloud_, same as data. This is the legitimate "long-lived state + orchestration" case for a real workflow.

**A singleton `ProxyControlWorkflow` per install** (Workflow ID `proxy-control`, in the install's namespace) holds desired state:

```
ProxyControlState { enabled: boolean, devices: EdgeConfig[], version: long }
```

- **Cloud UI drives it via signals** (egress client call): `enable()`, `disable()`, `applyConfig(devices)` / `upsertDevice` / `removeDevice`. Signal handlers **validate** the proposed config and reject bad changes with a clear message before they go live.
- **Proxy reads it via query**: a `ProxyControlPoller` bean queries `getState()` on a short interval; a `Reconciler` **hot-applies** changes — starts/stops ingress listeners, builds/tears down connectors, applies routing, flips enabled — **with no restart**.
- **Continue-as-new** periodically to bound history.

> **On/off = soft disable:** stop ingress listeners and pause outbound processing, but keep the lightweight control poller (and the egress connection) alive so the cloud can turn it back **on**. This is the only remotely-_reversible_ form of "off."
>
> **Config source of truth:** Temporal holds _operational_ config (devices, routing, enabled — hot-updatable). Only _bootstrap_ config (namespace, certs, target) stays local.

---

# PART 1 — Proxy Application (Java + Spring Boot + Maven) · ✅ COMPLETE

The full agnostic connector plus a minimal harness to demo it. Runs against the always-on Docker Temporal stack (`localhost:7233`, Server 1.31.1) or the CLI dev server (`just temporal-dev`).

## 1.1 Project & build

- **Maven** single module, `pom.xml`. Java 17+. Spring Boot 3.x.
- **Dependencies:**
  - `io.temporal:temporal-spring-boot-starter` (GA artifact, latest 1.35.x — **not** the deprecated `-alpha`; transitively pulls `temporal-sdk` + autoconfigure)
  - `org.springframework.boot:spring-boot-starter-web` (HTTP ingress + admin endpoints)
  - `org.springframework.boot:spring-boot-starter-validation`
  - TCP: plain Java NIO `ServerSocketChannel` (no extra dep) or **Netty** if framing gets complex
  - FTP: **Apache FtpServer** (`org.apache.ftpserver:ftpserver-core`) for the ingress drop-server + `WatchService`; **Apache Commons Net** (`commons-net`) `FTPClient` for outbound
  - Test: `spring-boot-starter-test`, Temporal `temporal-testing`

> Use the **`temporal-developer` skill** to scaffold the Temporal pieces and to confirm the **standalone-activity** client API in the current Java SDK.

## 1.2 `application.yml` (bootstrap only)

Two profiles. **`local`** targets the dev server (no mTLS); **`cloud`** targets Temporal Cloud with mTLS.

```yaml
# default / cloud
spring:
  temporal:
    connection:
      target: ${TEMPORAL_TARGET} # <ns-id>.<region>.tmprl.cloud:7233
      mtls:
        key-file: ${TEMPORAL_KEY_FILE}
        cert-chain-file: ${TEMPORAL_CERT_FILE}
    namespace: ${TEMPORAL_NAMESPACE} # <tenant>.<account-id>
    workers-auto-discovery:
      packages: [com.proxyapp.temporal, com.proxyapp.control]
proxy:
  task-queue: proxy-main
  profile: warehouse # which message catalog/profile to load
  ingress:
    http-port: 8080
---
spring:
  config.activate.on-profile: local
  temporal:
    connection:
      target: 127.0.0.1:7233
    namespace: default
```

- Activities/workflows via auto-discovery (`@ActivityImpl` / `@WorkflowImpl`, ordinary Spring beans). Inject `WorkflowClient` with `@Autowired`.
- **Never** set `insecure-trust-manager`/`server-name` against Cloud. Use single-namespace config (avoid `spring.temporal.namespaces[]`).

## 1.3 Package structure (`com.proxyapp`)

```
config/        TemporalConfig; bootstrap ProxyProperties (@ConfigurationProperties)
control/       ProxyControlWorkflow (+Impl)   # singleton: {enabled, devices[]} via signals + query
               ProxyControlPoller             # queries control wf on an interval
               Reconciler                     # diff desired vs running; start/stop listeners + connectors
routing/       MessageType, Direction (CLOUD_TO_EDGE | EDGE_TO_CLOUD)
               EdgeConfig, RouteBinding, Channel (port|path|folder)
               RouteTable                     # resolve (type,dir) -> connector target / ingress channel -> cloud endpoint
               MessageCatalog                 # the loaded profile (types, codecs, cloud endpoints)
               DeviceTemplate                 # per-edge-model profiles
               ConfigValidator                # pool membership + channel-collision checks
               MessageTypeResolver (SPI)       # FilenamePatternResolver impl, opt-in
temporal/
  workflow/    DeliverToEdgeWorkflow (+Impl)  # outbound: cloud starts this, proxy worker runs it
  activity/    DeliverToEdgeActivity (+Impl)  # outbound: TransmitToDevice — route -> connector send
               DeliverToCloudActivity (+Impl) # inbound: standalone activity -> POST to cloud endpoint
connector/     Connector (SPI: send), HttpConnector, TcpConnector, FtpConnector, ConnectorFactory
codec/         MessageCodec (SPI), JsonCodec (default; FixedWidth/Xml later)
ingress/       HttpIngressController, TcpSocketServer, FtpIngressListener (SmartLifecycle beans)
               InboundGateway                 # channel -> type -> decode -> start DeliverToCloud -> ack
profile/       built-in profiles, incl. WarehouseProfile (the reference example)
model/         canonical message DTOs
```

**SPIs**

- `Connector { void send(ChannelTarget target, byte[] payload); }`
- `MessageCodec { byte[] encode(CanonicalMessage m); CanonicalMessage decode(byte[] raw); }`
- `MessageTypeResolver { MessageType resolve(InboundContext ctx); }` (opt-in)

**Ingress coexistence.** HTTP via Spring MVC (path = channel). `TcpSocketServer` and `FtpIngressListener` are `SmartLifecycle` beans with their **own** `ExecutorService` accept-loops (never Tomcat/Temporal poller threads); the `Reconciler` opens/closes them as config changes. Listeners stay thin — channel → `InboundGateway` → ack.

## 1.4 Test harness (`dummy-cloud` + `dummy-edge`)

> **Superseded:** the original separate-repos decision was replaced by a **monorepo** — the dummies now live in `dummy-cloud/` and `dummy-edge/` beside `proxy/`, built by the root aggregator pom.

Each is a Spring Boot + Maven app running the **Warehouse reference profile**. They exist only to exercise the proxy; the `justfile` runs them.

- **`dummy-cloud`** (port `8081`) — Temporal **client**. REST/`/demo/*` endpoints to start `DeliverToEdge` for any message type; control-plane drivers (`enable`/`disable`/`applyConfig` signals) to exercise remote on/off + hot reconfig; REST endpoints per inbound type that receive `DeliverToCloud` and log/store. Copies the catalog + DTOs.
- **`dummy-edge`** (port `8082`) — exposes all three transports with per-type channels (HTTP paths, TCP ports, FTP folders). On an outbound message it auto-generates the paired confirm and pushes it back over the same transport/channel.

## 1.5 Build order (Part 1)

1. ~~**Routing core** — `MessageType`/`Direction`, `EdgeConfig`/`RouteBinding`/`Channel`, `MessageCatalog`, `RouteTable`, `ConfigValidator`, and the `WarehouseProfile`. Pure logic, **unit-tested first** (no Temporal needed).~~ ✅
2. ~~**Data path (HTTP)** — Temporal starter + bootstrap config, `DeliverToEdge` workflow + `DeliverToCloud` activity, `Connector` SPI + `HttpConnector`, `JsonCodec`, `HttpIngressController` + `InboundGateway`.~~ ✅
3. ~~**Dummies (HTTP)** — `dummy-cloud` + `dummy-edge` for the pick-task / pick-confirm pair → **first end-to-end demo** (`just demo-http`).~~ ✅
4. ~~**Control plane** — `ProxyControlWorkflow` + `ProxyControlPoller` + `Reconciler`; drive `enable`/`disable`/`applyConfig` from `dummy-cloud`.~~ ✅
5. ~~**Transports** — `TcpConnector`/`TcpSocketServer` (port pool), then `FtpConnector`/`FtpIngressListener` (folders); extend dummies. Add remaining message types via the catalog.~~ ✅
6. ~~**Polish** — device templates + opt-in `FilenamePatternResolver`; per-transport reliability docs.~~ ✅

## 1.6 Verification (Part 1) ✅

Verified against the always-on Docker Temporal (Server 1.31.1, `localhost:7233`). Temporal UI at `http://localhost:8080`.

- ~~**E2E (HTTP):** `just demo-http` → trigger `WAVE_RELEASE` from `dummy-cloud` → `dummy-edge` receives on `/pick-tasks` → it pushes `PICK_CONFIRM` to the proxy's `/pick-confirm` channel → confirm lands at `dummy-cloud`'s pick-confirm endpoint. Inspect the `DeliverToEdge` workflow + `TransmitToDevice` activity in the Temporal UI.~~ ✅
- ~~**Per type & transport:** repeat for put-away and cycle-count pairs over TCP (ports) and FTP (folders); verify channel-based type resolution with no payload inspection.~~ ✅
- ~~**Control plane:** `disable()` → ingress stops & outbound pauses, proxy stays connected → `enable()` resumes. `applyConfig` adding/moving a channel → hot reload, no restart. Push an invalid config (port collision / out of pool) → validation rejects with a clear message.~~ ✅
- ~~**Idempotency:** duplicate outbound start and duplicate edge push → exactly one execution each.~~ ✅
- **Proxy-down outbound durability:** stop proxy, dispatch a message, restart → delivers on reconnect. _(not yet verified)_
- **Egress-only:** confirm the proxy reaches Temporal with only outbound connectivity. _(not yet verified)_

---

# PART 2 — Management UI (Next.js) · **NOT STARTED**

A standalone web app for operations and solutions consultants to manage a proxy install: lifecycle control, route configuration, and live Temporal visibility — without touching code or a terminal.

> ### Design goal
>
> The primary user is a **solutions consultant** (warehouse domain, not deeply technical). The UI must be simple enough that the dev team is not involved in day-to-day config changes. Technical depth (Temporal event history, raw JSON) is available but tucked behind progressive disclosure — the happy path stays clean.

## 2.1 Project & stack

- **Next.js 14+** (App Router), TypeScript.
- **ShadCN** component primitives + **custom Tailwind** for a clean, original UI. No off-the-shelf dashboard templates, no generic AI-generated layouts. The design should feel intentional and product-grade — custom color palette, purposeful spacing, typographic hierarchy. Build a component library from ShadCN primitives that reflects the product's identity.
- Lives in the monorepo as `management-ui/`. Own `package.json`; no Maven involvement.
- **Justfile recipes:** `just run-ui` (dev server), `just build-ui` (production build).
- **Port:** `3000` (default Next.js dev port — no conflict with the 7233/8080/8090–8092 range).

## 2.2 Architecture

> ### Key constraint: the UI never talks to the proxy directly.
>
> The proxy is **on-prem behind a firewall**. The only network path out is the Temporal egress gRPC channel. The management UI runs on the **cloud side**. Every interaction between the UI and the proxy flows **through Temporal** — signals down, queries up. This means IT never opens an inbound port for management traffic; the only thing they maintain is the Temporal Cloud connection.

```
         CLOUD SIDE                           │  ON-PREM (firewall)
                                              │
  ┌──────────────────────────┐                │  ┌─────────────┐
  │  Management UI (:3000)   │                │  │  Proxy app  │
  │                          │                │  │  (:8090)     │
  │  React ←→ API routes     │                │  │             │
  │        │                 │                │  │  Control    │
  │   @temporalio/client     │                │  │  poller ←──────── queries/signals
  │        │                 │                │  │             │
  └────────┼─────────────────┘                │  └──────┬──────┘
           │                                  │         │
           ▼                                  │         │ egress gRPC only
  ┌──────────────────┐                        │         │
  │  Temporal Server  │◄──────────────────────│─────────┘
  │  (:7233)          │                       │
  └──────────────────┘                        │
           ▲                                  │
  ┌────────┴─────────┐                        │  ┌─────────────┐
  │  dummy-cloud     │  (demo dispatch        │  │  dummy-edge  │
  │  (:8091)         │   via REST — both      │  │  (:8092)     │
  └──────────────────┘   on cloud side)       │  └─────────────┘
```

**Backend (Next.js API routes) — every call flows through Temporal:**

| Concern | How |
|---|---|
| **Control plane** (enable, disable, applyConfig, upsertDevice, removeDevice) | `@temporalio/client` — signal the `proxy-control` workflow |
| **Control state** (enabled, devices, version, lastError) | `@temporalio/client` — query `getState` on the control workflow |
| **Proxy applied status** (listeners, applied version, health) | `@temporalio/client` — query the control workflow (proxy reports applied state back via signals; see §2.3) |
| **Proxy lifecycle** (stop, restart) | `@temporalio/client` — signal `shutdown` / `restart` on the control workflow (proxy acts on it locally; see §2.3) |
| **Temporal visibility** (workflow list, activity list, event history) | `@temporalio/client` — `WorkflowService` visibility APIs |
| **Demo dispatch** (fire test messages) | REST → dummy-cloud `/demo/*` endpoints (both on the cloud side — no firewall crossing) |

**The browser never talks to Temporal or any backend directly** — all external calls go through Next.js server-side API routes, keeping credentials and network topology off the client.

## 2.3 Proxy lifecycle management

Everything goes through Temporal. No direct REST calls to the proxy.

### Proxy-side changes (control workflow extensions)

The `ProxyControlWorkflow` gains new signals and enriched query state:

**New signals:**
- `shutdown` — the proxy's control poller detects this, initiates graceful JVM shutdown (equivalent to Actuator shutdown, but triggered via Temporal). The supervisor wrapper restarts the process.
- `restart` — sugar for shutdown-then-start: the poller shuts down the JVM, the supervisor relaunches it. The restarted proxy reconnects to Temporal and resumes polling.

**Enriched query state:** The proxy's `ProxyControlPoller` / `Reconciler` signals back its **applied status** after each reconciliation:
- Applied config version (confirms the proxy has caught up to desired state)
- Active listeners (HTTP paths, TCP ports, FTP folders)
- Process uptime / last-seen timestamp
- Health (healthy / degraded / unreachable — the UI infers "unreachable" when last-seen exceeds a threshold)

This lets the UI show live proxy state without ever reaching the proxy's network. The query returns both **desired** state (what the cloud pushed) and **applied** state (what the proxy reports).

### Two tiers of control in the UI

| Action | Mechanism | What happens |
|---|---|---|
| **Soft disable** | Signal `disable` on the control workflow | Data flow stops (ingress listeners shut down, outbound pauses). Process stays alive, control poller stays connected. Instantly reversible. |
| **Soft enable** | Signal `enable` on the control workflow | Data flow resumes. |
| **Hard restart** | Signal `restart` on the control workflow | Proxy poller detects the signal, initiates graceful JVM shutdown. Supervisor wrapper relaunches. UI shows a "restarting..." state and polls the query until the proxy reports back. |
| **Hard stop** | Signal `shutdown` on the control workflow | Same as restart, but the supervisor is configured to stay down (or the UI signals disable first, then shutdown). |

**Proxy-side infra:**
- Add `spring-boot-starter-actuator` to the proxy (the poller calls `SpringApplication.exit()` internally when it receives a shutdown/restart signal — Actuator is used for graceful shutdown hooks, not as an exposed endpoint).
- `just run-proxy-managed` recipe: a wrapper script that runs the proxy in a restart-on-exit loop. Shutdown exits the JVM → wrapper relaunches. For production, this role is filled by systemd / Windows Service.

## 2.4 Route configuration (guided wizard)

The target user is a solutions consultant who knows the warehouse domain but isn't deeply technical. The flow prioritizes **guided simplicity over raw power**. Advanced users can drop to a JSON editor.

### Wizard flow (3 steps)

**Step 1 — Choose a device template.** Select from the profile's built-in templates (e.g., "Standard MHE" for the warehouse profile). The template pre-fills message types, transports, and channel layout — the most common case needs zero manual binding.

**Step 2 — Set site values.** Fill in the site-specific fields the template can't know:
- Device ID (a friendly name like `conveyor-east`)
- Base URL / host (e.g., `http://192.168.1.50:8082`)
- FTP credentials (if FTP bindings exist)
- Base TCP port (the template applies offsets from this; the UI shows the resulting ports and highlights any pool conflicts)

**Step 3 — Review & apply.** A summary card shows every binding:

| Message type | Direction | Transport | Channel | Target |
|---|---|---|---|---|
| WAVE_RELEASE | Cloud → Edge | HTTP | /pick-tasks | http://192.168.1.50:8082/pick-tasks |
| PICK_CONFIRM | Edge → Cloud | HTTP | /pick-confirm | (proxy listens) |
| ... | | | | |

**Validation runs client-side AND server-side** (mirrors `ConfigValidator`): port-pool membership, no channel collisions, transport/kind agreement, direction checks. Errors are inline, human-readable (e.g., "TCP port 7777 is outside the available pool 6000–6010"), not stack traces.

**Apply** sends an `upsertDevice` signal to the control workflow. The UI polls the control state until `version` increments (confirming the proxy reconciled) and shows a success toast. If `lastError` is set, it displays the rejection reason.

### Advanced mode

A toggle exposes:
- Raw JSON editor for the full `EdgeConfig` (for power users / edge cases the wizard doesn't cover)
- `applyConfig` (replace all devices at once)
- `removeDevice` by ID

## 2.5 Temporal dashboard (live visibility)

The goal: a non-technical user can see "what's flowing" without opening the Temporal CLI or the raw Temporal UI.

### Activity feed

A live-updating table of recent `DeliverToEdge` workflows and `DeliverToCloud` standalone activities:

| Timestamp | Type | Message | Direction | Status | Duration |
|---|---|---|---|---|---|
| 3:48 PM | WAVE_RELEASE-ORD-3001 | DeliverToEdge | Cloud → Edge | Completed | 161ms |
| 3:48 PM | PICK_CONFIRM-ORD-3001 | DeliverToCloud | Edge → Cloud | Completed | 45ms |

- **Color-coded status**: green (completed), yellow (running), red (failed/timed out).
- **Click to expand**: shows the workflow/activity event history — scheduled, started, completed, with timestamps. For `DeliverToEdge` workflows, shows the `TransmitToDevice` activity inside.
- **Auto-refresh** on a short interval (2–5s), with a pause button.

### Proxy status panel

Queries the control workflow (both desired and applied state). Shows at a glance:
- **Connection indicator**: green (proxy reporting in), amber (last seen > threshold), red (unreachable)
- **Enabled/disabled** state
- **Active listeners**: HTTP paths, TCP ports, FTP folders the proxy has applied
- **Config version**: desired vs applied (shows if the proxy is behind)
- **Last error** (if a config push was rejected)

### Control workflow inspector

Progressive disclosure — collapsed by default. Expands to show the `proxy-control` workflow's current state, event count, and continue-as-new history.

## 2.6 Screens (summary)

1. **Dashboard** — proxy status panel (connection, enabled, listeners, config version) + activity feed (recent workflows/activities) + quick-action buttons (enable/disable/restart).
2. **Route Config** — device list with current bindings; "Add Device" launches the wizard; edit/remove existing devices. Advanced toggle for raw JSON.
3. **Temporal** — full activity feed with filtering (by type, direction, status, time range) + expandable event history. Control workflow inspector.
4. **Demo** — test dispatch buttons (fire a WAVE_RELEASE, CONTAINER_PUTAWAY, or CYCLE_COUNT_REQ with sample payloads). Shows the resulting workflow/activity in the feed in real time. _(Wired to dummy-cloud's `/demo/*` endpoints — both on the cloud side.)_

## 2.7 Build order (Part 2)

1. **Scaffold** — Next.js app with ShadCN + custom Tailwind theme. Justfile recipes. `@temporalio/client` connecting to `localhost:7233`. Establish the visual language (palette, typography, spacing, component patterns).
2. **Control plane** — enable/disable/restart via signals. Query control workflow state. Dashboard with status panel + quick-action buttons.
3. **Proxy status reporting** — extend `ProxyControlWorkflow` with applied-state signals. Update the poller to report back. UI shows desired vs applied state and connection health.
4. **Temporal dashboard** — activity feed listing recent workflows/activities via visibility APIs. Click-to-expand event history.
5. **Route config wizard** — template picker → site values → review → apply. Validation mirroring `ConfigValidator`.
6. **Demo panel** — test dispatch buttons wired to dummy-cloud endpoints. Live feed showing the result.
7. **Polish** — loading states, error handling, toasts, responsive layout, keyboard accessibility.

## 2.8 Verification (Part 2)

- **Zero direct proxy access:** confirm no network calls from the UI or its API routes to the proxy's IP/port. All state comes through Temporal queries; all commands go through Temporal signals.
- **Lifecycle:** soft disable/enable via UI → proxy status reflects in the control workflow query. Hard restart via signal → proxy goes down, comes back, applied-state query resumes.
- **Route config:** clone a device template, change the base URL, apply → proxy reconciles, applied state shows new listeners. Push an invalid config → inline error, desired state unchanged.
- **Visibility:** fire a test dispatch from the demo panel → workflow appears in the activity feed within seconds, click to see the `TransmitToDevice` activity in the event history.
- **Persona test:** a non-developer can add a new edge device using only the wizard (no JSON, no terminal) in under 2 minutes.
- **Design review:** the UI does not look like a generic dashboard template. Custom visual identity, purposeful layout, polished interactions.

---

# PART 3 — Hardening & Rollout

- **More codecs:** real edge formats — fixed-width, XML, delimited — behind `MessageCodec`. Per-(edge,type) codec selection.
- **More transports / framing:** TCP length-prefixed & delimiter framing; FTP/SFTP; HTTP auth schemes.
- **Profile & template library:** ready-made profiles beyond Warehouse; device templates for common edge models; import/export.
- **Observability:** structured logging, metrics (per-type throughput, retries, reconcile events), health endpoints; surface activity failures back to the cloud.
- **On-prem packaging (no Docker):** runnable `java -jar` + a service unit (systemd/Windows service) and a config bootstrap (namespace, certs, target). Document the install/runbook and the per-transport reliability profile.
- **Multi-tenant onboarding:** scripted namespace + cert provisioning on the cloud side (out of this repo, but documented).

---

## Appendix — Local ports

> Updated for the always-on Docker Temporal stack (`~/git/temporal/docker-compose.yml`):
> its Web UI owns 8080, so the demo apps moved to 8090–8092.

| Component                          | Port      |
| ---------------------------------- | --------- |
| Temporal server (Docker, gRPC)     | 7233      |
| Temporal Web UI (Docker)           | 8080      |
| Temporal Web UI (`just temporal-dev` fallback) | 8233 |
| Proxy HTTP ingress / admin         | 8090      |
| dummy-cloud                        | 8091      |
| dummy-edge                         | 8092      |
| Management UI (Next.js)            | 3000      |
| Proxy TCP inbound pool (example)   | 6000–6010 |

See `justfile` for build and demo recipes.
