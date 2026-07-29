package com.sems.battery;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates a home battery bank.
 *  - GetBatteryStatus (unary): instantaneous snapshot.
 *  - ReportChargeCycles (client streaming): accepts a stream of charge/discharge
 *    events and returns one aggregated summary once the client finishes streaming.
 */
public class BatteryStorageServiceImpl extends BatteryStorageServiceGrpc.BatteryStorageServiceImplBase {

    /** In-memory simulated battery state, keyed by battery_id. */
    private static final class BatteryState {
        double capacity = 10.0;      // kWh
        double currentLevel = 6.0;   // kWh
        double health = 100.0;       // %
        int cumulativeCycles = 0;
    }

    private final Map<String, BatteryState> batteries = new ConcurrentHashMap<>();

    private BatteryState stateFor(String batteryId) {
        return batteries.computeIfAbsent(batteryId, id -> new BatteryState());
    }

    @Override
    public void getBatteryStatus(StatusRequest request, StreamObserver<StatusResponse> responseObserver) {
        if (request.getBatteryId() == null || request.getBatteryId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("battery_id must not be empty")
                    .asRuntimeException());
            return;
        }

        BatteryState state = stateFor(request.getBatteryId());
        StatusResponse response = StatusResponse.newBuilder()
                .setBatteryId(request.getBatteryId())
                .setCapacity(state.capacity)
                .setCurrentLevel(state.currentLevel)
                .setHealth(state.health)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<ChargeEvent> reportChargeCycles(StreamObserver<ChargeSummary> responseObserver) {
        return new StreamObserver<>() {
            private String batteryId = null;
            private int totalEvents = 0;
            private double sumCharge = 0.0;

            @Override
            public void onNext(ChargeEvent event) {
                if (event.getChargePercent() < 0.0 || event.getChargePercent() > 100.0) {
                    // Input validation (Section 5.1): reject out-of-range percentages.
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("charge_percent must be between 0 and 100, got " + event.getChargePercent())
                            .asRuntimeException());
                    return;
                }

                batteryId = event.getBatteryId();
                totalEvents++;
                sumCharge += event.getChargePercent();

                // Update the simulated battery, clamped to [0, 100] as a percentage of capacity.
                BatteryState state = stateFor(batteryId);
                double clamped = Math.max(0.0, Math.min(100.0, event.getChargePercent()));
                state.currentLevel = state.capacity * (clamped / 100.0);
                state.cumulativeCycles++;
                // Simulated wear: health drops slightly with each cycle.
                state.health = Math.max(0.0, 100.0 - state.cumulativeCycles * 0.02
                        - ThreadLocalRandom.current().nextDouble(0, 0.05));
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("ReportChargeCycles stream error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                if (totalEvents == 0) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("No charge events were received")
                            .asRuntimeException());
                    return;
                }
                ChargeSummary summary = ChargeSummary.newBuilder()
                        .setBatteryId(batteryId)
                        .setTotalEvents(totalEvents)
                        .setAvgCharge(Math.round((sumCharge / totalEvents) * 10.0) / 10.0)
                        .build();
                responseObserver.onNext(summary);
                responseObserver.onCompleted();
            }
        };
    }
}
