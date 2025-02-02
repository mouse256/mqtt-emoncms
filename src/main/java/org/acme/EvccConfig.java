package org.acme;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Map;

@ConfigMapping(prefix = "evcc")
public interface EvccConfig {
    @WithDefault("true")
    boolean enabled();

    Map<String, Loadpoint> loadpoints();

    interface Loadpoint {
        @WithName("charge_total_import")
        String chargeTotalImport();

        @WithName("charge_power")
        String chargePower();

        @WithName("charge_current")
        String chargeCurrent();

        @WithName("charge_current_1")
        String chargeCurrent1();

        @WithName("charge_current_2")
        String chargeCurrent2();

        @WithName("charge_current_3")
        String chargeCurrent3();

        @WithName("phases_active")
        String phasesActive();
    }
}