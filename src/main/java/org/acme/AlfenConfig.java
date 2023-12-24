package org.acme;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "alfen")
public interface AlfenConfig {
    Input input();

    interface Input {
        Map<String, Properties> properties();

        interface Properties {
            Map<String, Map<String, String>> category();
        }
    }

}