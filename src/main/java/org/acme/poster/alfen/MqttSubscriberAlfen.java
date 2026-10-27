package org.acme.poster.alfen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.vertx.mqtt.messages.MqttPublishMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.acme.EmonPosterCache;
import org.acme.MqttSubscriber;
import org.acme.data.PropertyParsed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@ApplicationScoped
public class MqttSubscriberAlfen implements MqttSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final ObjectReader propertiesReader;
    private final AlfenConfig alfenConfig;
    private static final String PREFIX = "alfen/modbus/state/";
    private static final Pattern PROPERTIES_REGEX = Pattern.compile("^" + PREFIX + "(\\S+)/(\\d+)/(\\S+)$");
    private final EmonPosterCache emonPoster;
    private final ObjectMapper objectMapper;

    public MqttSubscriberAlfen(ObjectMapper objectMapper, AlfenConfig alfenConfig, EmonPosterCache emonPoster) {
        LOG.info("Creating mqtt subscriber for Alfen");
        this.propertiesReader = objectMapper.readerForListOf(PropertyParsed.class);
        this.objectMapper = objectMapper;
        this.alfenConfig = alfenConfig;
        this.emonPoster = emonPoster;
        if (alfenConfig.enabled()) {
            emonPoster.start("Alfen");
        }
    }

    @Override
    public List<String> getSubscriptions() {
        return List.of(PREFIX + "#");
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public void consume(MqttPublishMessage msg) {
        if (!alfenConfig.enabled()) {
            return;
        }
        try {
            LOG.debug("Incoming message on: {}", msg.topicName());
            Matcher matcher = PROPERTIES_REGEX.matcher(msg.topicName());
            if (matcher.matches()) {
                String meterName = matcher.group(1);
                String meterId = matcher.group(2);
                String catetegoryName = matcher.group(3);
                AlfenConfig.Input.Properties propertiesConfig = alfenConfig.input().properties().get(meterName);
                if (propertiesConfig == null) {
                    LOG.debug("No input handled for this meter: {}: ({})", meterName, msg.topicName());
                    return;
                }
                Map<String, String> categoryConfig = propertiesConfig.category().get(catetegoryName);
                if (categoryConfig == null) {
                    LOG.debug("No input handled for this category: {} ({})", catetegoryName, msg.topicName());
                    return;
                }
                Map<String, Object> values  = objectMapper.readValue(msg.payload().getBytes(), Map.class);
                LOG.debug("Received values: {}", values);
//                List<PropertyParsed> properties = propertiesReader.readValue(msg.payload().getBytes());
                Map<String, Object> data = values.entrySet().stream()
                        .filter(p -> categoryConfig.containsKey(p.getKey()))
                        .collect(Collectors.toMap(p -> categoryConfig.get(p.getKey()), Map.Entry::getValue));
                LOG.debug("Postring: {}: {}", meterName, data);
                emonPoster.add(meterName, data);
            } else {
                LOG.warn("Don't know how to handle topic {}", msg.topicName());
            }
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.topicName(), e);
        }
    }
}
