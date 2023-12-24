package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.acme.data.PropertyParsed;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@ApplicationScoped
public class MqttSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Vertx vertx;
    private final MqttConfig mqttConfig;
    private final ObjectMapper objectMapper;
    private final ObjectReader propertiesReader;
    private final AlfenConfig alfenConfig;
    private final EmoncmsConfig emoncmsConfig;
    private static final Pattern PROPERTIES_REGEX = Pattern.compile("^alfen/properties/(\\S+)/category/(\\S+)$");
    private HttpClient httpClient;

    public MqttSubscriber(Vertx vertx, MqttConfig mqttConfig, ObjectMapper objectMapper, AlfenConfig alfenConfig, EmoncmsConfig emoncmsConfig) {
        this.vertx = vertx;
        this.mqttConfig = mqttConfig;
        this.objectMapper = objectMapper;
        this.propertiesReader = objectMapper.readerForListOf(PropertyParsed.class);
        this.alfenConfig = alfenConfig;
        this.emoncmsConfig = emoncmsConfig;
    }

    public void onStart(@Observes StartupEvent startupEvent) {
        LOG.info("Startup");
        if (!mqttConfig.enabled()) {
            LOG.warn("MQTT is disabled");
            return;
        }
        LOG.info("emoncms: {}", emoncmsConfig.endpoint());
        httpClient = HttpClient.newBuilder().build();
    }

    void onShutdown(@Observes ShutdownEvent event) {
        LOG.info("Shutdown");
        httpClient.shutdown();
    }


    @Incoming("alfen")
    CompletionStage<Void> consume(MqttMessage<byte[]> msg) {
        try {
            LOG.info("Incoming message on: {}", msg.getTopic());
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
                post(meterName, data);
            } else {
                LOG.warn("Don't know how to handle topic {}", msg.getTopic());
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.nack(e);
        }
    }

    private void post(String device, Map<String, Object> keys) {
        String data = getEmoncmsData(device, keys);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(emoncmsConfig.endpoint() + "/input/post"))
                .headers("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(data))
                .build();


        try {
            LOG.info("POST: {} -- {}", request.uri(), data);
            HttpResponse<String> response = httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString());
            LOG.info("http response: {} -- {}", response.statusCode(), response.body());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String getEmoncmsData(String device, Map<String, Object> keys) {
        try {
            String data = objectMapper.writeValueAsString(keys);
            return getFormDataAsString(Map.of("node", device,
                    "apikey", emoncmsConfig.apikey(),
                    "fulljson", data));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getFormDataAsString(Map<String, String> formData) {
        StringBuilder formBodyBuilder = new StringBuilder();
        for (Map.Entry<String, String> singleEntry : formData.entrySet()) {
            if (!formBodyBuilder.isEmpty()) {
                formBodyBuilder.append("&");
            }
            formBodyBuilder.append(URLEncoder.encode(singleEntry.getKey(), StandardCharsets.UTF_8));
            formBodyBuilder.append("=");
            formBodyBuilder.append(URLEncoder.encode(singleEntry.getValue(), StandardCharsets.UTF_8));
        }
        return formBodyBuilder.toString();
    }
}
