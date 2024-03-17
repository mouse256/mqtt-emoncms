package org.acme;

import io.smallrye.config.ConfigMapping;

import java.util.List;

@ConfigMapping(prefix = "qbus")
public interface QbusConfig {
    List<String> types();
}