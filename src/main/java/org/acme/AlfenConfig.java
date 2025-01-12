package org.acme;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Map;

@ConfigMapping(prefix = "alfen")
public interface AlfenConfig {
    @WithDefault("true")
    boolean enabled();
    Input input();

    interface Input {
        Map<String, Properties> properties();

        interface Properties {
            Map<String, Map<String, String>> category();
        }
    }

}