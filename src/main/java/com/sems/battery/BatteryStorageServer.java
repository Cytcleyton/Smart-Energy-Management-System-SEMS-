package com.sems.battery;

import com.sems.common.AuthServerInterceptor;
import com.sems.discovery.ServiceRegistrar;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/** Independent process hosting the BatteryStorageService and advertising it via JmDNS. */
public class BatteryStorageServer {

    private static final int PORT = 50052;

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new BatteryStorageServiceImpl())
                .intercept(new AuthServerInterceptor())
                .build();

        server.start();
        System.out.println("BatteryStorageService listening on port " + PORT);

        try (ServiceRegistrar registrar = new ServiceRegistrar()) {
            registrar.register("_battery._tcp.local.", "BatteryStorageService", PORT, "Home Battery Bank gRPC Service");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down BatteryStorageService...");
                server.shutdown();
            }));

            server.awaitTermination();
        }
    }
}
