package org.acme;

import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;


@ApplicationScoped
public class MqttSubscriberEvcc {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final EvccConfig evccConfig;
    private final EmonPosterCache emonPoster;
    private static final String DEVICE = "alfen1";


    public MqttSubscriberEvcc(EvccConfig evccConfig, EmonPosterCache emonPoster) {
        this.evccConfig = evccConfig;
        this.emonPoster = emonPoster;
        emonPoster.setName("Evcc");
    }


    @Incoming("evcc")
    CompletionStage<Void> consume(MqttMessage<byte[]> msg) {
        if (!evccConfig.enabled()) {
            LOG.debug("Evcc is disabled");
            return msg.ack();
        }
        try {
            LOG.debug("Incoming message on: {}", msg.getTopic());
            String[] topicParts = msg.getTopic().split("/");
            if (topicParts.length < 4) {
                LOG.debug("Nothing to do with this message");
                return msg.ack();
            }
            if (!"loadpoints".equals(topicParts[1])) {
                LOG.debug("Not a loadpoints message");
                return msg.ack();
            }
            String id = topicParts[2];
            EvccConfig.Loadpoint loadpoint = evccConfig.loadpoints().get(id);
            if (loadpoint == null) {
                LOG.debug("No loadpoint found for id: {}", id);
                return msg.ack();
            }
            String type = topicParts[3];
            String meterConfig = switch (type) {
                case "chargeCurrent" -> loadpoint.chargeCurrent();
                case "chargePower" -> loadpoint.chargePower();
                case "chargeTotalImport" -> loadpoint.chargeTotalImport();
                case "phasesActive" -> loadpoint.phasesActive();
                case "chargeCurrents" -> {
                    if (topicParts.length != 5) {
                        LOG.debug("Not a chargeCurrents message, too short");
                        yield null;
                    }
                    yield switch (topicParts[4]) {
                        case "l1" -> loadpoint.chargeCurrent1();
                        case "l2" -> loadpoint.chargeCurrent2();
                        case "l3" -> loadpoint.chargeCurrent3();
                        default -> null;
                    };
                }
                default -> null;
            };
            String payload = new String(msg.getPayload());
            if (meterConfig != null && !payload.isEmpty()) {
                double value = Double.parseDouble(payload);
                LOG.debug("Value for {} -> {}: {}", DEVICE, meterConfig, value);
                emonPoster.add(DEVICE, meterConfig, value);
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.ack();
            //return msg.nack(e);
        }
    }

}
