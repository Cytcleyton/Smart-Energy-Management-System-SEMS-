package com.sems.discovery;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Browses the local network for a given mDNS service type and resolves the
 * host/port of the first instance found. Used by the GUI/console client so
 * that no service address is ever hard-coded.
 */
public class ServiceLocator implements AutoCloseable {

    private final JmDNS jmdns;
    private final ConcurrentHashMap<String, ServiceInfo> discovered = new ConcurrentHashMap<>();

    public ServiceLocator() throws IOException {
        this.jmdns = JmDNS.create(InetAddress.getLocalHost());
    }

    /**
     * Blocks (up to timeoutSeconds) until at least one instance of serviceType
     * is discovered, then returns its resolved ServiceInfo (host + port).
     */
    public ServiceInfo discover(String serviceType, int timeoutSeconds) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        ServiceListener listener = new ServiceListener() {
            @Override
            public void serviceAdded(ServiceEvent event) {
                jmdns.requestServiceInfo(event.getType(), event.getName(), 5000);
            }

            @Override
            public void serviceResolved(ServiceEvent event) {
                discovered.put(event.getType(), event.getInfo());
                latch.countDown();
            }

            @Override
            public void serviceRemoved(ServiceEvent event) {
                discovered.remove(event.getType());
            }
        };

        jmdns.addServiceListener(serviceType, listener);
        boolean found = latch.await(timeoutSeconds, TimeUnit.SECONDS);
        jmdns.removeServiceListener(serviceType, listener);

        if (!found) {
            throw new IllegalStateException("No instance of " + serviceType + " found within " + timeoutSeconds + "s. "
                    + "Is the service running and advertising on this network?");
        }
        return discovered.get(serviceType);
    }

    @Override
    public void close() {
        try {
            jmdns.close();
        } catch (IOException e) {
            System.err.println("Error closing JmDNS: " + e.getMessage());
        }
    }
}
