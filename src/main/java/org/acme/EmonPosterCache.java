package org.acme;

import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

@ApplicationScoped
public class EmonPosterCache {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Vertx vertx;
    private final Map<String, Map<String, Object>> valuesForDevices = new HashMap<>();
    private final EmonPoster emonPoster;


    public EmonPosterCache(Vertx vertx, EmonPoster emonPoster) {
        this.emonPoster = emonPoster;
        this.vertx = vertx;
        vertx.setPeriodic(Duration.ofSeconds(5).toMillis(), this::sendInfo);
    }

    public void add(String device, Map<String, ?> keys) {
        synchronized (valuesForDevices) {
            if (!valuesForDevices.containsKey(device)) {
                valuesForDevices.put(device, new HashMap<>());
            }
            Map<String, Object> values = valuesForDevices.get(device);
            values.putAll(keys);
        }
    }

    private void sendInfo(Long l) {
        LOG.info("Sending info to emoncms");
        vertx.executeBlocking((Callable<Void>) () -> {
            Map<String, Map<String, Object>> localValues;
            synchronized (valuesForDevices) {
                localValues = new HashMap<>(valuesForDevices);
            }
            localValues.forEach(emonPoster::post);

            return null;
        });
    }

    public void add(String device, String key, Object value) {
        add(device, Map.of(key, value));
    }
}
