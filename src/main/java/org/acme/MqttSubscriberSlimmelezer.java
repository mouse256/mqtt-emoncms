package org.acme;

import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@ApplicationScoped
public class MqttSubscriberSlimmelezer {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final SlimmelezerConfig slimmelezerConfig;
    private static final Pattern PROPERTIES_REGEX = Pattern.compile("^slimmelezer/sensor/(\\S+)/state$");
    private final EmonPosterCache emonPoster;
    private static final String DEVICE = "slimmelezer";
    private final Map<String, Double> values = new HashMap<>();


    public MqttSubscriberSlimmelezer(SlimmelezerConfig slimmelezerConfig, EmonPosterCache emonPoster) {
        this.slimmelezerConfig = slimmelezerConfig;
        this.emonPoster = emonPoster;
    }


    @Incoming("slimmelezer")
    CompletionStage<Void> consume(MqttMessage<byte[]> msg) {
        try {
            LOG.debug("Incoming message on: {}", msg.getTopic());
            Matcher matcher = PROPERTIES_REGEX.matcher(msg.getTopic());
            if (matcher.matches()) {
                String meterName = matcher.group(1);
                String meterConfig = slimmelezerConfig.items().get(meterName);
                if (meterConfig == null) {
                    LOG.debug("No input handled for this meter: {}: ({})", meterName, msg.getTopic());
                    return msg.ack();
                }
                double value = Double.parseDouble(new String(msg.getPayload()));
                emonPoster.add(DEVICE, meterConfig, value);
            } else {
                LOG.debug("Don't know how to handle topic {}", msg.getTopic());
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.nack(e);
        }
    }

}
