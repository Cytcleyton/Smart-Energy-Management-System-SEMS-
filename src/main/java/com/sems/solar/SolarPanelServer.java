package com.sems.solar;

import com.sems.common.AuthServerInterceptor;
import com.sems.discovery.ServiceRegistrar;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/** Independent process hosting the SolarPanelService and advertising it via JmDNS. */
public class SolarPanelServer {

    private static final int PORT = 50051;

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new SolarPanelServiceImpl())
                .intercept(new AuthServerInterceptor())
                .build();

        server.start();
        System.out.println("SolarPanelService listening on port " + PORT);

        try (ServiceRegistrar registrar = new ServiceRegistrar()) {
            registrar.register("_solar._tcp.local.", "SolarPanelService", PORT, "Solar Panel Array gRPC Service");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down SolarPanelService...");
                server.shutdown();
            }));

            server.awaitTermination();
        }
    }
}
