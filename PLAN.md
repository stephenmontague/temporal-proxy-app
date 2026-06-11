# Cloud ↔ Edge Proxy — Design & Build Plan

> A **domain-agnostic, durable connector** that bridges any **Cloud** application and any **Edge** target (an on-prem device, machine, or network), in both directions, with **Temporal Cloud** as the durable backbone.
>
> The warehouse case (a cloud WMS ↔ on-prem MHE) is used throughout as the **reference example / demo profile** — it is _not_ baked into the core. Swap the profile and the same proxy connects anything cloud-side to anything on-prem.

---

## How to use this document

This plan is split into **three parts**:

- **Part 1 — Proxy Application (Java + Spring Boot + Maven).** The agnostic connector itself, plus a minimal `dummy-cloud` / `dummy-edge` harness to demo it end-to-end. **This is the focus — build this first.**
- **Part 2 — Management UI (Next.js).** A web app the cloud-app operator uses to remotely enable/disable a proxy install and edit its routing config. **Deferred — do not start until Part 1 is solid.**
- **Part 3 — Hardening & rollout.** Additional transports/codecs, profile library, observability, and on-prem packaging.

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

**Cloud → Edge (outbound).** The cloud client starts a standalone `DeliverToEdge` activity (Activity ID = `{messageType}-{businessId}`, dedup policy `REJECT_DUPLICATES` + `USE_EXISTING`) on the install's task queue. The proxy worker polls over egress gRPC and executes it: look up the route for `(type, CLOUD_TO_EDGE)` → select a `Connector` (HTTP/TCP/FTP) + `MessageCodec` → encode → send to the edge target's channel for that type. Temporal handles retries/timeouts; the connector send must tolerate replay.

> **Durability win:** the task lands in Temporal Cloud first, so if the proxy is offline the work waits and delivers on reconnect.

**Edge → Cloud (inbound).** The edge target pushes to a proxy ingress **channel**. The proxy identifies the message type **from the channel, never from the payload** (see §3), then starts a standalone `DeliverToCloud` activity (Activity ID = `{messageType}-{businessId}`, same dedup). The proxy worker runs it, POSTing to that type's cloud endpoint with retries.

> **Reliability levers (cheap, kept):**
>
> - **Ack-after-enqueue** — ack the edge target only _after_ Temporal accepts the activity start, so a retrying device behaves correctly.
> - **Per-transport reliability is documented** — FTP is inherently store-and-forward; raw TCP/HTTP push leans on device retry.
>
> **Out of scope:** no local durable spool. The SDK's auto-reconnecting gRPC channel + Temporal Cloud's 99.9% SLA cover transient cloud-unreachability; the only residual risk (proxy host down) is an HA/shared-responsibility concern a spool can't fix.

### Key design principles

- **Standalone Activities** (Temporal Public Preview — Cloud + CLI v1.7.0+, Server v1.31.0+, Java SDK) are the primitive for single durable side-effecting calls. **Reach for a real workflow only when genuine orchestration is needed** (multi-step, fan-out, long-lived state, saga). The control plane (§4) is the one place that earns a workflow.
- **Activity ID = `{messageType}-{businessId}`** is the dedup handle both directions — collapses duplicate cloud starts and duplicate edge pushes to one execution (exactly-once delivery on top of at-least-once activities).
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

# PART 1 — Proxy Application (Java + Spring Boot + Maven) · **FOCUS**

The full agnostic connector plus a minimal harness to demo it. Build and demo entirely with the local Temporal dev server (no Docker).

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
  activity/    DeliverToEdgeActivity (+Impl)  # outbound: route -> connector send
               DeliverToCloudActivity (+Impl) # inbound: route -> POST to cloud endpoint
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

1. **Routing core** — `MessageType`/`Direction`, `EdgeConfig`/`RouteBinding`/`Channel`, `MessageCatalog`, `RouteTable`, `ConfigValidator`, and the `WarehouseProfile`. Pure logic, **unit-tested first** (no Temporal needed).
2. **Data path (HTTP)** — Temporal starter + bootstrap config, `DeliverToEdge`/`DeliverToCloud` activities, `Connector` SPI + `HttpConnector`, `JsonCodec`, `HttpIngressController` + `InboundGateway`.
3. **Dummies (HTTP)** — `dummy-cloud` + `dummy-edge` for the pick-task / pick-confirm pair → **first end-to-end demo** (`just demo-http`).
4. **Control plane** — `ProxyControlWorkflow` + `ProxyControlPoller` + `Reconciler`; drive `enable`/`disable`/`applyConfig` from `dummy-cloud`.
5. **Transports** — `TcpConnector`/`TcpSocketServer` (port pool), then `FtpConnector`/`FtpIngressListener` (folders); extend dummies. Add remaining message types via the catalog.
6. **Polish** — device templates + opt-in `FilenamePatternResolver`; per-transport reliability docs.

## 1.6 Verification (Part 1)

Run against the **local dev server** (`just temporal-dev`). Use the **`temporal-developer` skill** for CLI inspection (`temporal workflow list`, activity describe, etc.).

- **E2E (HTTP):** `just demo-http` → trigger `WAVE_RELEASE` from `dummy-cloud` → `dummy-edge` receives on `/pick-tasks` → it pushes `PICK_CONFIRM` to the proxy's `/pick-confirm` channel → confirm lands at `dummy-cloud`'s pick-confirm endpoint. Inspect both activities in the Temporal UI (`http://localhost:8233`).
- **Per type & transport:** repeat for put-away and cycle-count pairs over TCP (ports) and FTP (folders); verify channel-based type resolution with no payload inspection.
- **Control plane:** `disable()` → ingress stops & outbound pauses, proxy stays connected → `enable()` resumes. `applyConfig` adding/moving a channel → hot reload, no restart. Push an invalid config (port collision / out of pool) → validation rejects with a clear message.
- **Idempotency:** duplicate outbound start and duplicate edge push → exactly one execution each.
- **Proxy-down outbound durability:** stop proxy, dispatch a message, restart → delivers on reconnect.
- **Egress-only:** confirm the proxy reaches Temporal with only outbound connectivity.

---

# PART 2 — Management UI (Next.js) · **DEFERRED**

A web app the cloud-app operator uses to manage proxy installs. **Do not build yet** — captured here only so Part 1's control-plane contract anticipates it.

- **Stack:** Next.js (App Router). Talks to a thin cloud-side API that holds a Temporal **client** and forwards UI actions as **signals** to the relevant install's `ProxyControlWorkflow`, and reads status via **query**.
- **Screens (sketch):** install list + on/off toggle; edge-target list with the 3-layer config (catalog read-only, infra fields, routing bindings); guided channel picker (port pool / paths / folders) with client+server validation mirroring `ConfigValidator`; device-template clone flow; live status (enabled, version, last reconcile) from the control workflow query.
- **Boundary:** the UI never talks to a proxy directly — only to the cloud API, which signals Temporal. Keeps the egress-only security model intact.

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
| Proxy TCP inbound pool (example)   | 6000–6010 |

See `justfile` for build and demo recipes.
