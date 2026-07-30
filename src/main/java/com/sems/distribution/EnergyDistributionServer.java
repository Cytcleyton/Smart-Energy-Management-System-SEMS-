package com.sems.distribution;

import com.sems.common.AuthServerInterceptor;
import com.sems.discovery.ServiceRegistrar;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/** Independent process hosting the EnergyDistributionService and advertising it via JmDNS. */
public class EnergyDistributionServer {

    private static final int PORT = 50053;

    public static void main(String[] args) throws Exception {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new EnergyDistributionServiceImpl())
                .intercept(new AuthServerInterceptor())
                .build();

        server.start();
        System.out.println("EnergyDistributionService listening on port " + PORT);

        try (ServiceRegistrar registrar = new ServiceRegistrar()) {
            registrar.register("_distribution._tcp.local.", "EnergyDistributionService", PORT, "Household Grid Controller gRPC Service");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down EnergyDistributionService...");
                server.shutdown();
            }));

            server.awaitTermination();
        }
    }
}
