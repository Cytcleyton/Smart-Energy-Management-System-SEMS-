# Smart Energy Management System (SEMS) — Part 2 Implementation

Java/gRPC implementation of the three-service microgrid described in the Part 1 proposal.

## Project layout

```
sems/
├── pom.xml
└── src/main/
    ├── proto/                          # .proto contracts (unchanged from Part 1)
    │   ├── solarpanel.proto
    │   ├── battery_storage.proto
    │   └── energy_distribution.proto
    └── java/com/sems/
        ├── solar/          SolarPanelServiceImpl + SolarPanelServer   (port 50051)
        ├── battery/        BatteryStorageServiceImpl + BatteryStorageServer (port 50052)
        ├── distribution/   EnergyDistributionServiceImpl + EnergyDistributionServer (port 50053)
        ├── discovery/      ServiceRegistrar (JmDNS publish) / ServiceLocator (JmDNS browse)
        ├── common/         AuthUtil + client/server interceptors (Section 5.3 metadata)
        └── client/         ClientApp — console controller exercising all four RPC types
```

Each service is an independent process, matches its `.proto` exactly, and advertises itself
over JmDNS so the client never hard-codes a host or port (Section 4 of the proposal).

## Build (Gradle / IntelliJ)

Requires **JDK 17+**. Open the project in IntelliJ as a Gradle project; it will download
grpc-java, protobuf, and JmDNS automatically the first time you sync.

From a terminal:

```bash
./gradlew build
```

This also runs the protobuf Gradle plugin, which compiles the three `.proto` files into
Java sources (message classes + `*Grpc` stub classes) under `build/generated/source/proto`.

## Run

Start all three servers first (each in IntelliJ's Run panel, or a separate terminal), then the client. See the step-by-step guide below for exactly how to do this in IntelliJ.

`ClientApp` will:
1. Discover all three services via JmDNS (no hard-coded addresses).
2. Call each unary RPC with a 3-second client-side deadline.
3. Open `StreamEnergyOutput`, print 5 live readings, then cancel the call.
4. Stream 3 `ChargeEvent`s to `ReportChargeCycles` and print the returned summary.
5. Open `NegotiateDistribution`, send offers from two sources, print the allocations.
6. Send a deliberately invalid request to show `INVALID_ARGUMENT` being caught and handled.

## What's implemented vs. what's left

Implemented (matches Sections 3–5 of the proposal):
- All four gRPC types, one per pair of RPCs, as specified.
- JmDNS registration and discovery (Section 4).
- Input validation + gRPC status codes: `INVALID_ARGUMENT` (Section 5.1).
- Client-side deadlines on unary calls, client-side cancellation on the streaming call (5.2).
- Simulated `Authorization: Bearer <token>` metadata, checked server-side and rejected with
  `UNAUTHENTICATED` if missing (5.3).
- Simulated energy logic: fluctuating solar output, battery wear/clamping, priority-based
  distribution allocation (Section 3 "Example Logic").

Not yet built — the JavaFX GUI from Section 6. `ClientApp` is a console stand-in that
exercises the exact same calls the GUI would make; happy to build the JavaFX front end
next if you want the full GUI controller for the final submission.

## Suggested Git workflow (Section 7 requirement: incremental commit history)

Commit in stages rather than one upload, e.g.:
1. `.proto` files + generated build config
2. SolarPanelService (impl + server)
3. BatteryStorageService (impl + server)
4. EnergyDistributionService (impl + server)
5. JmDNS discovery + auth interceptors
6. Client
7. README / final polish
