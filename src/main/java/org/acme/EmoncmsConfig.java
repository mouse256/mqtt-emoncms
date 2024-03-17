package org.acme;

import io.smallrye.config.ConfigMapping;

import java.util.Optional;

@ConfigMapping(prefix = "emoncms")
public interface EmoncmsConfig {
    String endpoint();

    String apikey();

    Optional<Boolean> enabled();

}