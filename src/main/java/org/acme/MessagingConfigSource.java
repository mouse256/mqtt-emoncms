package org.acme;


import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MessagingConfigSource implements ConfigSource {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Map<String, String> properties = new HashMap<>();

    public MessagingConfigSource() {
        LOG.info("Initializing MessagingConfigSource");
        Properties config = System.getProperties();

        // Set the global MQTT host
        String globalHost = config.getProperty("mqtt.host");
        LOG.info("Global host: {}", globalHost);
        LOG.info("Global pot: {}", config.getProperty("mqtt.port"));

        // Scan for MQTT channels dynamically
        properties.keySet().forEach(key -> {
            LOG.info("Config property: {}", key);
            if (key.startsWith("mp.messaging.incoming.") && key.endsWith(".connector")){
                String value = config.getProperty(key);
                if (value.equals("smallrye-mqtt")) {
                    LOG.info("MQTT channel found: {}", key);
                    // Extract the channel name
                    String channelName = key.substring("mp.messaging.incoming.".length(), key.lastIndexOf(".connector"));

                    // Set the host property for this channel
                    properties.put("mp.messaging.incoming." + channelName + ".host", globalHost);
                }
            }
        });
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public String getValue(String propertyName) {
        return properties.get(propertyName);
    }

    @Override
    public String getName() {
        return "DynamicMessagingConfigSource";
    }
}

