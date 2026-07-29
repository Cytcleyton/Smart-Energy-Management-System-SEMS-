package com.sems.solar;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates a rooftop solar panel array.
 *  - GetPanelStatus (unary): one-off health check.
 *  - StreamEnergyOutput (server streaming): continuous wattage feed, ~1 reading/sec,
 *    until the client cancels the call (e.g. closes the GUI panel).
 */
public class SolarPanelServiceImpl extends SolarPanelServiceGrpc.SolarPanelServiceImplBase {

    @Override
    public void getPanelStatus(PanelStatusRequest request, StreamObserver<PanelStatusResponse> responseObserver) {
        if (request.getPanelId() == null || request.getPanelId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("panel_id must not be empty")
                    .asRuntimeException());
            return;
        }

        // Simulated health check: panel is operational with a realistic efficiency value.
        double efficiency = 85.0 + ThreadLocalRandom.current().nextDouble() * 13.0; // 85.0 - 98.0 %
        PanelStatusResponse response = PanelStatusResponse.newBuilder()
                .setPanelId(request.getPanelId())
                .setOperational(true)
                .setEfficiency(Math.round(efficiency * 10.0) / 10.0)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void streamEnergyOutput(StreamRequest request, StreamObserver<EnergyReading> responseObserver) {
        if (request.getPanelId() == null || request.getPanelId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("panel_id must not be empty")
                    .asRuntimeException());
            return;
        }

        ServerCallStreamObserver<EnergyReading> serverObserver =
                (ServerCallStreamObserver<EnergyReading>) responseObserver;
        AtomicBoolean cancelled = new AtomicBoolean(false);
        serverObserver.setOnCancelHandler(() -> cancelled.set(true));

        Thread streamer = new Thread(() -> {
            long start = System.currentTimeMillis();
            try {
                while (!cancelled.get() && !serverObserver.isCancelled()) {
                    double elapsedSeconds = (System.currentTimeMillis() - start) / 1000.0;
                    // Simulated weather/time-of-day factor: a smooth curve plus noise.
                    double timeFactor = Math.max(0, Math.sin(elapsedSeconds / 20.0));
                    double watts = 300.0 * timeFactor + ThreadLocalRandom.current().nextDouble(-15, 15);
                    watts = Math.max(0.0, watts);

                    EnergyReading reading = EnergyReading.newBuilder()
                            .setWatts(Math.round(watts * 10.0) / 10.0)
                            .setTimestamp(System.currentTimeMillis() / 1000)
                            .build();

                    serverObserver.onNext(reading);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (!cancelled.get()) {
                    responseObserver.onCompleted();
                }
            }
        }, "solar-stream-" + request.getPanelId());
        streamer.setDaemon(true);
        streamer.start();
    }
}
