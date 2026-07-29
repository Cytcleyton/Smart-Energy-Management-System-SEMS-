package com.sems.common;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Rejects calls that don't carry the simulated device-authentication token
 * in gRPC metadata (Section 5.3). Real deployments would validate a signed
 * token per device rather than a single shared constant.
 */
public class AuthServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String token = headers.get(AuthUtil.AUTH_HEADER);
        if (token == null || !token.equals(AuthUtil.DEVICE_TOKEN)) {
            call.close(Status.UNAUTHENTICATED.withDescription("Missing or invalid authorization token"), new Metadata());
            return new ServerCall.Listener<>() {
            };
        }

        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
        };
    }
}
