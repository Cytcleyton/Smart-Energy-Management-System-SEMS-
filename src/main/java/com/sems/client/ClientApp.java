package com.sems.client;

import com.sems.battery.BatteryStorageServiceGrpc;
import com.sems.battery.ChargeEvent;
import com.sems.battery.ChargeSummary;
import com.sems.battery.StatusRequest;
import com.sems.battery.StatusResponse;
import com.sems.common.AuthClientInterceptor;
import com.sems.discovery.ServiceLocator;
import com.sems.distribution.DistributionRequest;
import com.sems.distribution.DistributionResponse;
import com.sems.distribution.Empty;
import com.sems.distribution.EnergyDistributionServiceGrpc;
import com.sems.distribution.GridStatusResponse;
import com.sems.solar.EnergyReading;
import com.sems.solar.PanelStatusRequest;
import com.sems.solar.PanelStatusResponse;
import com.sems.solar.SolarPanelServiceGrpc;
import com.sems.solar.StreamRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import javax.jmdns.ServiceInfo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Console-based controller that stands in for the JavaFX GUI described in the proposal
 * (Section 6). It performs the same interaction flow: discover every service via JmDNS,
 * then exercise all four gRPC communication types against them, with client-side
 * deadlines, cancellation, and gRPC-status-aware error handling.
 *
 * Run this only after all three servers (SolarPanelServer, BatteryStorageServer,
 * EnergyDistributionServer) are already running.
 */
public class ClientApp {

    public static void main(String[] args) throws Exception {
        try (ServiceLocator locator = new ServiceLocator()) {

            System.out.println("Discovering services via JmDNS...");
            ManagedChannel solarChannel = discoverOrFallback(locator, "_solar._tcp.local.", 50051);
            ManagedChannel batteryChannel = discoverOrFallback(locator, "_battery._tcp.local.", 50052);
            ManagedChannel distributionChannel = discoverOrFallback(locator, "_distribution._tcp.local.", 50053);

            try {
                runUnaryDemo(solarChannel, batteryChannel, distributionChannel);
                runServerStreamingDemo(solarChannel);
                runClientStreamingDemo(batteryChannel);
                runBidiStreamingDemo(distributionChannel);
                runErrorHandlingDemo(solarChannel);
            } finally {
                solarChannel.shutdown();
                batteryChannel.shutdown();
                distributionChannel.shutdown();
            }
        }
    }

    private static ManagedChannel buildChannel(ServiceInfo info) {
        String host = info.getHostAddresses()[0];
        int port = info.getPort();
        System.out.printf("  -> %s resolved to %s:%d%n", info.getName(), host, port);
        return ManagedChannelBuilder.forAddress(host, port)
                .intercept(new AuthClientInterceptor())
                .usePlaintext()
                .build();
    }

    /**
     * Tries real JmDNS discovery first (proving Section 4's "no hard-coded address"
     * requirement actually works end-to-end). If discovery can't find the service
     * within the timeout - which can happen on some networks/routers that restrict
     * mDNS multicast traffic - falls back to the well-known localhost port each
     * server binds to, so the demo can still run to completion.
     */
    private static ManagedChannel discoverOrFallback(ServiceLocator locator, String serviceType, int fallbackPort) {
        try {
            return buildChannel(locator.discover(serviceType, 8));
        } catch (Exception e) {
            System.out.printf("  JmDNS discovery for %s failed (%s) - falling back to localhost:%d%n",
                    serviceType, e.getMessage(), fallbackPort);
            return ManagedChannelBuilder.forAddress("localhost", fallbackPort)
                    .intercept(new AuthClientInterceptor())
                    .usePlaintext()
                    .build();
        }
    }

    /** Demonstrates the three unary RPCs, each issued with a client-side deadline (Section 5.2). */
    private static void runUnaryDemo(ManagedChannel solar, ManagedChannel battery, ManagedChannel distribution) {
        System.out.println("\n=== Unary RPCs (with 3s client-side deadlines) ===");

        SolarPanelServiceGrpc.SolarPanelServiceBlockingStub solarStub =
                SolarPanelServiceGrpc.newBlockingStub(solar).withDeadlineAfter(3, TimeUnit.SECONDS);
        PanelStatusResponse panelStatus = solarStub.getPanelStatus(
                PanelStatusRequest.newBuilder().setPanelId("panel-01").build());
        System.out.println("GetPanelStatus -> " + panelStatus);

        BatteryStorageServiceGrpc.BatteryStorageServiceBlockingStub batteryStub =
                BatteryStorageServiceGrpc.newBlockingStub(battery).withDeadlineAfter(3, TimeUnit.SECONDS);
        StatusResponse batteryStatus = batteryStub.getBatteryStatus(
                StatusRequest.newBuilder().setBatteryId("battery-01").build());
        System.out.println("GetBatteryStatus -> " + batteryStatus);

        EnergyDistributionServiceGrpc.EnergyDistributionServiceBlockingStub distributionStub =
                EnergyDistributionServiceGrpc.newBlockingStub(distribution).withDeadlineAfter(3, TimeUnit.SECONDS);
        GridStatusResponse gridStatus = distributionStub.getGridStatus(Empty.newBuilder().build());
        System.out.println("GetGridStatus -> " + gridStatus);
    }

    /** Demonstrates server streaming: reads a few live readings, then cancels the call. */
    private static void runServerStreamingDemo(ManagedChannel solar) throws InterruptedException {
        System.out.println("\n=== Server Streaming: StreamEnergyOutput (5 readings, then cancel) ===");

        SolarPanelServiceGrpc.SolarPanelServiceStub asyncStub = SolarPanelServiceGrpc.newStub(solar);
        CountDownLatch latch = new CountDownLatch(1);
        int[] count = {0};

        io.grpc.stub.ClientResponseObserver<StreamRequest, EnergyReading> observer =
                new io.grpc.stub.ClientResponseObserver<>() {
                    private io.grpc.stub.ClientCallStreamObserver<StreamRequest> requestStream;

                    @Override
                    public void beforeStart(io.grpc.stub.ClientCallStreamObserver<StreamRequest> requestStream) {
                        this.requestStream = requestStream;
                    }

                    @Override
                    public void onNext(EnergyReading reading) {
                        System.out.println("  reading -> " + reading);
                        count[0]++;
                        if (count[0] >= 5) {
                            requestStream.cancel("Client is done watching the feed", null);
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        Status status = Status.fromThrowable(t);
                        if (status.getCode() != Status.Code.CANCELLED) {
                            System.err.println("  stream error: " + status);
                        }
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

        asyncStub.streamEnergyOutput(StreamRequest.newBuilder().setPanelId("panel-01").build(), observer);
        latch.await(8, TimeUnit.SECONDS);
    }

    /** Demonstrates client streaming: streams several charge events, then reads the one summary. */
    private static void runClientStreamingDemo(ManagedChannel battery) throws InterruptedException {
        System.out.println("\n=== Client Streaming: ReportChargeCycles ===");

        BatteryStorageServiceGrpc.BatteryStorageServiceStub asyncStub = BatteryStorageServiceGrpc.newStub(battery);
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<ChargeEvent> requestObserver = asyncStub.reportChargeCycles(new StreamObserver<>() {
            @Override
            public void onNext(ChargeSummary summary) {
                System.out.println("  ChargeSummary -> " + summary);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("  stream error: " + Status.fromThrowable(t));
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        double[] events = {40.0, 55.0, 70.0};
        for (double percent : events) {
            requestObserver.onNext(ChargeEvent.newBuilder()
                    .setBatteryId("battery-01")
                    .setChargePercent(percent)
                    .setIsCharging(true)
                    .build());
        }
        requestObserver.onCompleted();

        latch.await(5, TimeUnit.SECONDS);
    }

    /** Demonstrates bidirectional streaming: sources report watts while allocations stream back. */
    private static void runBidiStreamingDemo(ManagedChannel distribution) throws InterruptedException {
        System.out.println("\n=== Bidirectional Streaming: NegotiateDistribution ===");

        EnergyDistributionServiceGrpc.EnergyDistributionServiceStub asyncStub =
                EnergyDistributionServiceGrpc.newStub(distribution);
        CountDownLatch latch = new CountDownLatch(1);

        StreamObserver<DistributionRequest> requestObserver = asyncStub.negotiateDistribution(new StreamObserver<>() {
            @Override
            public void onNext(DistributionResponse response) {
                System.out.println("  allocation -> " + response);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("  stream error: " + Status.fromThrowable(t));
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        requestObserver.onNext(DistributionRequest.newBuilder().setSourceId("panel-01").setWattsOffered(1200.0).build());
        Thread.sleep(200);
        requestObserver.onNext(DistributionRequest.newBuilder().setSourceId("battery-01").setWattsOffered(800.0).build());
        Thread.sleep(200);
        requestObserver.onCompleted();

        latch.await(5, TimeUnit.SECONDS);
    }

    /** Demonstrates gRPC status-code-aware error handling (Section 5.1) with a deliberately bad request. */
    private static void runErrorHandlingDemo(ManagedChannel solar) {
        System.out.println("\n=== Error Handling: invalid request ===");

        SolarPanelServiceGrpc.SolarPanelServiceBlockingStub solarStub = SolarPanelServiceGrpc.newBlockingStub(solar);
        try {
            solarStub.getPanelStatus(PanelStatusRequest.newBuilder().setPanelId("").build());
        } catch (StatusRuntimeException e) {
            System.out.println("  Caught expected error -> " + e.getStatus().getCode() + ": " + e.getStatus().getDescription());
        }
    }
}