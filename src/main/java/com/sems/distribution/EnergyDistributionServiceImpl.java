package com.sems.distribution;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EnergyDistributionServiceImpl extends EnergyDistributionServiceGrpc.EnergyDistributionServiceImplBase {

    /** Simulated household appliances waiting for an allocation in priority order. */
    private static final Map<String, Double> APPLIANCE_DEMAND = new LinkedHashMap<>();
    static {
        APPLIANCE_DEMAND.put("immersion-heater", 1500.0);
        APPLIANCE_DEMAND.put("ev-charger", 800.0);
        APPLIANCE_DEMAND.put("washing-machine", 700.0);
    }

    // Tracks cumulative watts offered per source across a household, per call.
    private final Map<String, Double> sourceOffers = new ConcurrentHashMap<>();

    // Tracks the most recent allocation sent to each appliance, so GetGridStatus can show
    // a live snapshot of what's currently allocated, not just raw totals.
    private final Map<String, DistributionResponse> lastAllocations = new ConcurrentHashMap<>();

    @Override
    public void getGridStatus(Empty request, StreamObserver<GridStatusResponse> responseObserver) {
        double totalDemand = APPLIANCE_DEMAND.values().stream().mapToDouble(Double::doubleValue).sum();
        double totalSupply = sourceOffers.values().stream().mapToDouble(Double::doubleValue).sum();

        GridStatusResponse.Builder responseBuilder = GridStatusResponse.newBuilder()
                .setDemand(totalDemand)
                .setSupply(totalSupply);

        sourceOffers.forEach((sourceId, watts) ->
                responseBuilder.addRegisteredSources(SourceStatus.newBuilder()
                        .setSourceId(sourceId)
                        .setWattsOffered(watts)
                        .build()));

        responseBuilder.addAllCurrentAllocations(lastAllocations.values());

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<DistributionRequest> negotiateDistribution(StreamObserver<DistributionResponse> responseObserver) {
        return new StreamObserver<>() {

            @Override
            public void onNext(DistributionRequest request) {
                if (request.getWattsOffered() < 0) {
                    // Input validation (Section 5.1): reject negative wattage.
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("watts_offered must not be negative, got " + request.getWattsOffered())
                            .asRuntimeException());
                    return;
                }

                // Pool this source's offer into the shared supply - sources are fungible in a
                // microgrid, the controller doesn't care which panel or battery a watt came from.
                sourceOffers.merge(request.getSourceId(), request.getWattsOffered(), (oldVal, newVal) -> newVal);

                // Re-run a full allocation pass across every appliance, in priority order, using the
                // total pooled supply. Every new offer immediately re-balances the whole microgrid -
                // real dynamic load balancing
                double remainingSupply = sourceOffers.values().stream().mapToDouble(Double::doubleValue).sum();

                for (Map.Entry<String, Double> entry : APPLIANCE_DEMAND.entrySet()) {
                    String appliance = entry.getKey();
                    double demand = entry.getValue();
                    double allocated = Math.max(0.0, Math.min(demand, remainingSupply));
                    remainingSupply -= allocated;
                    double wattsFromGrid = Math.max(0.0, demand - allocated);
                    boolean applianceNeedsGrid = wattsFromGrid > 0;

                    DistributionResponse response = DistributionResponse.newBuilder()
                            .setApplianceId(appliance)
                            .setWattsAllocated(Math.round(allocated * 10.0) / 10.0)
                            .setGridImportNeeded(applianceNeedsGrid)
                            .setWattsFromGrid(Math.round(wattsFromGrid * 10.0) / 10.0)
                            .build();

                    lastAllocations.put(appliance, response);
                    responseObserver.onNext(response);
                }
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
