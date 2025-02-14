package org.acme;

import io.vertx.core.Vertx;
import jakarta.enterprise.context.Dependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

@Dependent
public class EmonPosterCache {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Vertx vertx;
    private final Map<String, Map<String, Object>> valuesForDevices = new HashMap<>();
    private final EmonPoster emonPoster;
    private String name;

    public EmonPosterCache(Vertx vertx, EmonPoster emonPoster) {
        this.emonPoster = emonPoster;
        this.vertx = vertx;
        this.name = "";
        vertx.setPeriodic(Duration.ofSeconds(10).toMillis(), this::sendInfo);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void add(String device, Map<String, ?> keys) {
        LOG.debug("Adding values for {} (1): {}", device, keys);
        synchronized (valuesForDevices) {
            LOG.debug("Adding values for {} (2): {}", device, keys);
            if (!valuesForDevices.containsKey(device)) {
                valuesForDevices.put(device, new HashMap<>());
            }
            Map<String, Object> values = valuesForDevices.get(device);
            values.putAll(keys);
        }
    }

    private void sendInfo(Long l) {
        LOG.debug("[{}] Sending info to emoncms (1)", name);
        vertx.executeBlocking((Callable<Void>) () -> {
            LOG.debug("[{}] Sending info to emoncms (2)", name);
            Map<String, Map<String, Object>> localValues;
            synchronized (valuesForDevices) {
                LOG.debug("[{}] Sending info to emoncms (3)", name);
                if (valuesForDevices.isEmpty()) {
                    LOG.info("[{}] Stale, not sending info", name);
                    return null;
                }
                localValues = new HashMap<>(valuesForDevices);
                valuesForDevices.clear();
            }
            localValues.forEach(emonPoster::post);

            return null;
        });
    }

    public void add(String device, String key, Object value) {
        add(device, Map.of(key, value));
    }
}
