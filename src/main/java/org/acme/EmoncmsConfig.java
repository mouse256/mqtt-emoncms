package org.acme;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "emoncms")
public interface EmoncmsConfig {
    String endpoint();

    String apikey();

}