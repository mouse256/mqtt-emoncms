package org.acme;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.util.Map;

@ConfigMapping(prefix = "slimmelezer")
public interface SlimmelezerConfig {
    @WithDefault("true")
    boolean enabled();
    Map<String, String> items();
}