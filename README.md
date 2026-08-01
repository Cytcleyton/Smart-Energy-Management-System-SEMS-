Smart Energy Management System (SEMS)
Distributed energy monitoring and allocation system built with Java, gRPC, and jmDNS. The system coordinates solar power generation, battery storage, and grid energy distribution using three microservices and a desktop client.

Repository Structure
.
├── build.gradle
├── settings.gradle
├── gradlew
└── src/
└── main/
├── proto/
│   ├── solarpanel.proto
│   ├── BatteryStorageService.proto
│   └── EnergyDistributionSystem.proto
└── java/com/sems/
├── solar/          # SolarPanelServer & Service Implementation
├── battery/        # BatteryStorageServer & Service Implementation
├── distribution/   # EnergyDistributionServer & Service Implementation
├── discovery/      # JmDNS Registration & Discovery (ServiceRegistrar / ServiceLocator)
├── common/         # Auth interceptors and metadata utilities
└── client/         # ClientGui (Swing Controller) & ClientApp (Console Fallback)
Prerequisites & Build
JDK: 17 or higher
Build Tool: Gradle (wrapper included)
Building the Project
To compile protocol buffers and build the project sources, run:

Bash
./gradlew build
The Protobuf plugin will generate the stub classes inside build/generated/source/proto/.

How to Run
Start the Microservices:
Run each server class (via IntelliJ or separate terminal instances):
SolarPanelServer (Port 50051)
BatteryStorageServer (Port 50052)
EnergyDistributionServer (Port 50053)
Launch the Controller:
Run ClientGui.java to launch the Swing desktop interface.
Key Features Implemented
gRPC Communication Patterns:
Unary: GetPanelStatus (Solar) / GetBatteryStatus (Battery)
Server Streaming: StreamTelemetry (Solar wattage stream)
Client Streaming: RecordChargeCycle (Battery event aggregation)
Bidirectional Streaming: NegotiateDistribution (Live grid allocation stream)
Service Discovery: Automatic network registration and discovery via jmDNS with a default fallback to localhost if multicast is restricted.
Resilience & Advanced gRPC Features:
Client-side deadlines on unary calls.
Explicit stream cancellation handling on telemetry streams.
Standard gRPC error status propagation (INVALID_ARGUMENT, NOT_FOUND).
Simple token-based authentication using gRPC metadata interceptors.
Graphical User Interface: Built with Java Swing using SwingWorker background threads to prevent UI lockups during network streaming.