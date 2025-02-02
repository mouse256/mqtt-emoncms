package org.acme;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.List;

@ConfigMapping(prefix = "qbus")
public interface QbusConfig {
    @WithDefault("true")
    boolean enabled();

    List<String> types();
}