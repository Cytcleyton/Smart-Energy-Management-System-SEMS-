package com.sems.distribution;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Acts as the household grid controller.
 *  - GetGridStatus (unary): quick supply-vs-demand check.
 *  - NegotiateDistribution (bidirectional streaming): sources continuously report
 *    available watts; the service continuously responds with allocation decisions,
 *    prioritising sources with the most available watts before flagging a grid import.
 */
public class EnergyDistributionServiceImpl extends EnergyDistributionServiceGrpc.EnergyDistributionServiceImplBase {

    /** Simulated household appliances waiting for an allocation, in priority order. */
    private static final Map<String, Double> APPLIANCE_DEMAND = new LinkedHashMap<>();
    static {
        APPLIANCE_DEMAND.put("immersion-heater", 1500.0);
        APPLIANCE_DEMAND.put("ev-charger", 800.0);
        APPLIANCE_DEMAND.put("washing-machine", 700.0);
    }

    // Tracks cumulative watts offered per source across a household, per call.
    private final Map<String, Double> sourceOffers = new ConcurrentHashMap<>();

    @Override
    public void getGridStatus(Empty request, StreamObserver<GridStatusResponse> responseObserver) {
        double totalDemand = APPLIANCE_DEMAND.values().stream().mapToDouble(Double::doubleValue).sum();
        double totalSupply = sourceOffers.values().stream().mapToDouble(Double::doubleValue).sum();

        GridStatusResponse response = GridStatusResponse.newBuilder()
                .setDemand(totalDemand)
                .setSupply(totalSupply)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<DistributionRequest> negotiateDistribution(StreamObserver<DistributionResponse> responseObserver) {
        return new StreamObserver<>() {
            private final AtomicInteger applianceCursor = new AtomicInteger(0);
            private final String[] appliances = APPLIANCE_DEMAND.keySet().toArray(new String[0]);

            @Override
            public void onNext(DistributionRequest request) {
                if (request.getWattsOffered() < 0) {
                    // Input validation (Section 5.1): reject negative wattage.
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("watts_offered must not be negative, got " + request.getWattsOffered())
                            .asRuntimeException());
                    return;
                }

                // Record how much this source is currently offering (prioritised by amount offered).
                sourceOffers.merge(request.getSourceId(), request.getWattsOffered(), (oldVal, newVal) -> newVal);

                double totalAvailable = sourceOffers.values().stream().mapToDouble(Double::doubleValue).sum();
                double totalDemand = APPLIANCE_DEMAND.values().stream().mapToDouble(Double::doubleValue).sum();
                boolean gridImportNeeded = totalAvailable < totalDemand;

                // Allocate to the next appliance in the priority queue, capped by what this source offered.
                String appliance = appliances[applianceCursor.getAndUpdate(i -> (i + 1) % appliances.length)];
                double demand = APPLIANCE_DEMAND.get(appliance);
                double allocated = Math.min(demand, request.getWattsOffered());

                DistributionResponse response = DistributionResponse.newBuilder()
                        .setApplianceId(appliance)
                        .setWattsAllocated(Math.round(allocated * 10.0) / 10.0)
                        .setGridImportNeeded(gridImportNeeded)
                        .build();

                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("NegotiateDistribution stream error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
