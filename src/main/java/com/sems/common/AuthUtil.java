package com.sems.common;

import io.grpc.Metadata;

/**
 * Shared constants and helpers for the simulated device-authentication
 * metadata attached to every outgoing gRPC call (Section 5.3 of the proposal).
 */
public final class AuthUtil {

    public static final Metadata.Key<String> AUTH_HEADER =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /** Simulated device token. In a real deployment this would be issued per device. */
    public static final String DEVICE_TOKEN = "Bearer sems-device-demo-token";

    private AuthUtil() {
    }
}
