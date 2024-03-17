package org.acme;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.data.PropertyParsed;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@ApplicationScoped
public class MqttSubscriberAlfen {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final ObjectReader propertiesReader;
    private final AlfenConfig alfenConfig;
    private static final Pattern PROPERTIES_REGEX = Pattern.compile("^alfen/properties/(\\S+)/category/(\\S+)$");
    private final EmonPoster emonPoster;

    public MqttSubscriberAlfen(ObjectMapper objectMapper, AlfenConfig alfenConfig, EmonPoster emonPoster) {
        this.propertiesReader = objectMapper.readerForListOf(PropertyParsed.class);
        this.alfenConfig = alfenConfig;
        this.emonPoster = emonPoster;
    }

    @Incoming("alfen")
    CompletionStage<Void> consume(MqttMessage<byte[]> msg) {
        try {
            LOG.debug("Incoming message on: {}", msg.getTopic());
            Matcher matcher = PROPERTIES_REGEX.matcher(msg.getTopic());
            if (matcher.matches()) {
                String meterName = matcher.group(1);
                String catetegoryName = matcher.group(2);
                AlfenConfig.Input.Properties propertiesConfig = alfenConfig.input().properties().get(meterName);
                if (propertiesConfig == null) {
                    LOG.debug("No input handled for this meter: {}: ({})", meterName, msg.getTopic());
                    return msg.ack();
                }
                Map<String, String> categoryConfig = propertiesConfig.category().get(catetegoryName);
                if (categoryConfig == null) {
                    LOG.debug("No input handled for this category: {} ({})", catetegoryName, msg.getTopic());
                    return msg.ack();
                }
                List<PropertyParsed> properties = propertiesReader.readValue(msg.getPayload());
                Map<String, Object> data = properties.stream()
                        .filter(p -> categoryConfig.containsKey(p.id()))
                        .collect(Collectors.toMap(p -> categoryConfig.get(p.id()), PropertyParsed::value));
                emonPoster.post(meterName, data);
            } else {
                LOG.warn("Don't know how to handle topic {}", msg.getTopic());
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.nack(e);
        }
    }

}
