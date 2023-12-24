package org.acme;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "mqtt")
public interface MqttConfig {
    String host();

    int port();

    boolean enabled();

}