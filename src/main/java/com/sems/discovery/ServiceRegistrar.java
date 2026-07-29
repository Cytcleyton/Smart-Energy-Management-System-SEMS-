package com.sems.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import java.io.IOException;
import java.net.InetAddress;

/**
 * Wraps JmDNS so each gRPC service can advertise itself on the local network
 * without any hard-coded IP address or port in the client.
 */
public class ServiceRegistrar implements AutoCloseable {

    private final JmDNS jmdns;
    private ServiceInfo serviceInfo;

    public ServiceRegistrar() throws IOException {
        this.jmdns = JmDNS.create(InetAddress.getLocalHost());
    }

    /**
     * Registers a service under a given mDNS service type, e.g. "_solar._tcp.local.".
     *
     * @param serviceType mDNS service type, must end with "._tcp.local."
     * @param name        friendly instance name shown to browsers
     * @param port        TCP port the gRPC server is listening on
     * @param description short human-readable description, stored as mDNS text record
     */
    public void register(String serviceType, String name, int port, String description) throws IOException {
        serviceInfo = ServiceInfo.create(serviceType, name, port, description);
        jmdns.registerService(serviceInfo);
        System.out.printf("[JmDNS] Registered %s as '%s' on port %d%n", serviceType, name, port);
    }

    @Override
    public void close() {
        if (serviceInfo != null) {
            jmdns.unregisterService(serviceInfo);
        }
        jmdns.unregisterAllServices();
        try {
            jmdns.close();
        } catch (IOException e) {
            System.err.println("Error closing JmDNS: " + e.getMessage());
        }
    }
}
