package org.acme;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.mqtt.messages.MqttPublishMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@ApplicationScoped
public class MqttSubscriberQbus implements MqttSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Vertx vertx;
    private final MqttConfig mqttConfig;
    private static final String PREFIX = "qbus/";
    private static final Pattern TOPIC_INFO_REGEX = Pattern.compile("^" + PREFIX + "\\d+/info/outputs/(\\S+)$");
    private static final Pattern TOPIC_STATE_REGEX = Pattern.compile("^" + PREFIX + "\\d+/sensor/(\\S+)/(\\d+)/state$");
    private final ObjectMapper objectMapper;
    private HttpClient httpClient;
    private final EmonPoster emonPoster;
    private final QbusConfig qbusConfig;
    private final Map<String, Map<Integer, Info>> info = new HashMap<>();
    private final Map<String, Map<Integer, Integer>> values;

    public MqttSubscriberQbus(Vertx vertx, MqttConfig mqttConfig, QbusConfig qbusConfig, ObjectMapper objectMapper, EmonPoster emonPoster) {
        this.vertx = vertx;
        this.mqttConfig = mqttConfig;
        this.objectMapper = objectMapper;
        this.emonPoster = emonPoster;
        this.qbusConfig = qbusConfig;
        values = qbusConfig.types().stream().collect(Collectors.toMap(e -> e, e -> new HashMap<>()));
    }

    public void onStart(@Observes StartupEvent startupEvent) {
        LOG.info("Startup");
        if (!mqttConfig.enabled()) {
            LOG.warn("MQTT is disabled");
            return;
        }
        if (!qbusConfig.enabled()) {
            LOG.warn("QBUS is disabled");
            return;
        }
        vertx.setPeriodic(Duration.ofSeconds(5).toMillis(), this::sendInfo);
    }

    private void sendInfo(Long l) {
        LOG.debug("Sending info to emonCMS");
        synchronized (values) {
            synchronized (info) {
                values.forEach((type, v) -> {
                    Map<Integer, Info> infoMap = info.get(type);
                    if (infoMap == null) {
                        return;
                    }
                    String device = "qbus-" + type;
                    Map<String, Integer> dataToPost = v.entrySet().stream()
                            .flatMap(e -> {
                                Info i = infoMap.get(e.getKey());
                                if (i == null) {
                                    return Stream.empty();
                                }
                                return Stream.of(new EmonData(i.name(), e.getValue()));
                            }).collect(Collectors.toMap(x -> x.name, x -> x.value));
                    vertx.executeBlocking((Callable<Void>) () -> {
                        emonPoster.post(device, dataToPost);
                        return null;
                    });
                });
            }
        }
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public List<String> getSubscriptions() {
        return List.of(
                PREFIX + "+/info/outputs/#",
                PREFIX + "+/sensor/+/+/state"
        );
    }

    record EmonData(String name, Integer value) {
    }

    record Info(Integer id, String name) {
    }

    @Override
    public void consume(MqttPublishMessage msg) {
        if (!qbusConfig.enabled()) {
            return;
        }
        Matcher m = TOPIC_STATE_REGEX.matcher(msg.topicName());
        if (m.matches()) {
            consumeState(msg, m);
            return;
        }
        m = TOPIC_INFO_REGEX.matcher(msg.topicName());
        if (m.matches()) {
            consumeState(msg, m);
            return;
        }
        LOG.debug("Can't parse topic ", msg.topicName());
    }

    private void consumeInfo(MqttPublishMessage msg, Matcher m) {
        try {
            String type = m.group(1);
            if (!qbusConfig.types().contains(type)) {
                LOG.debug("Ignoring info type {} since not in config", type);
                return;
            }
            List<Info> myObjects = objectMapper.readValue(msg.payload().getBytes(), new TypeReference<>() {
            });
            LOG.debug("Qbus info on: {} -- {}", msg.topicName(), myObjects);
            LOG.info("Qbus info for type: {}", type);
            synchronized (info) {
                info.put(type, myObjects.stream().collect(Collectors.toMap(e -> e.id, e -> e)));
            }
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.topicName(), e);
        }
    }


    private void consumeState(MqttPublishMessage msg, Matcher m) {
        try {
            String type = m.group(1);
            if (!qbusConfig.types().contains(type)) {
                LOG.debug("Ignoring state on type {} since not in config", type);
                return;
            }
            Integer id = Integer.parseInt(m.group(2));

            int data = Integer.parseInt(msg.payload().toString(StandardCharsets.UTF_8));
            LOG.info("Qbus state on: {}: {}", msg.topicName(), data);
            synchronized (values) {
                values.get(type).put(id, data);
            }
        } catch (
                Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.topicName(), e);
        }
    }

}
