package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

@ApplicationScoped
public class EmonPoster {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final ObjectMapper objectMapper;
    private final EmoncmsConfig emoncmsConfig;
    private HttpClient httpClient;

    public EmonPoster(ObjectMapper objectMapper, EmoncmsConfig emoncmsConfig) {
        this.objectMapper = objectMapper;
        this.emoncmsConfig = emoncmsConfig;
    }

    public void onStart(@Observes StartupEvent startupEvent) {
        LOG.info("Startup");
        LOG.info("emoncms: {}", emoncmsConfig.endpoint());
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    void onShutdown(@Observes ShutdownEvent event) {
        LOG.info("Shutdown");
        httpClient.shutdown();
    }

    public void post(String device, Map<String, ?> keys) {
        String data = getEmoncmsData(device, keys);
        if (!emoncmsConfig.enabled().orElse(true)) {
            LOG.info("Emoncms: disabled, not posting");
            return;
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(emoncmsConfig.endpoint() + "/input/post"))
                .headers("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(data))
                .timeout(Duration.ofSeconds(30))
                .build();


        try {
            LOG.info("POST: {} -- {}", request.uri(), data);
            HttpResponse<String> response = httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString());
            LOG.info("http response: {} -- {}", response.statusCode(), response.body());
        } catch (Exception e) {
            LOG.warn("Error posting to emoncms", e);
        }

    }

    private String getEmoncmsData(String device, Map<String, ?> keys) {
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
