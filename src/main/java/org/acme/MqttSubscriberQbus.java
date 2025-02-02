package org.acme;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@ApplicationScoped
public class MqttSubscriberQbus {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Vertx vertx;
    private final MqttConfig mqttConfig;
    private static final Pattern TOPIC_INFO_REGEX = Pattern.compile("^qbus/\\d+/info/outputs/(\\S+)$");
    private static final Pattern TOPIC_STATE_REGEX = Pattern.compile("^qbus/\\d+/sensor/(\\S+)/(\\d+)/state$");
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

    record EmonData(String name, Integer value) {
    }

    record Info(Integer id, String name) {
    }

    @Incoming("qbusInfo")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    CompletionStage<Void> consumeInfo(MqttMessage<byte[]> msg) {
        if (!qbusConfig.enabled()) {
            return msg.ack();
        }
        try {
            Matcher m = TOPIC_INFO_REGEX.matcher(msg.getTopic());
            if (!m.matches()) {
                LOG.warn("Can't parse info topic: {}", msg.getTopic());
                return msg.ack();
            }
            String type = m.group(1);
            if (!qbusConfig.types().contains(type)) {
                LOG.debug("Ignoring info type {} since not in config", type);
                return msg.ack();
            }
            List<Info> myObjects = objectMapper.readValue(msg.getPayload(), new TypeReference<>() {
            });
            LOG.debug("Qbus info on: {} -- {}", msg.getTopic(), myObjects);
            LOG.info("Qbus info for type: {}", type);
            synchronized (info) {
                info.put(type, myObjects.stream().collect(Collectors.toMap(e -> e.id, e -> e)));
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.nack(e);
        }
    }

    @Incoming("qbusState")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    CompletionStage<Void> consumeState(MqttMessage<byte[]> msg) {
        if (!qbusConfig.enabled()) {
            return msg.ack();
        }
        try {
            Matcher m = TOPIC_STATE_REGEX.matcher(msg.getTopic());
            if (!m.matches()) {
                LOG.warn("Can't parse state topic: {}", msg.getTopic());
                return msg.nack(new IllegalArgumentException("can't parse topic"));
            }
            String type = m.group(1);
            if (!qbusConfig.types().contains(type)) {
                LOG.debug("Ignoring state on type {} since not in config", type);
                return msg.ack();
            }
            Integer id = Integer.parseInt(m.group(2));

            int data = Integer.parseInt(new String(msg.getPayload()));
            LOG.info("Qbus state on: {}: {}", msg.getTopic(), data);
            synchronized (values) {
                values.get(type).put(id, data);
            }
            return msg.ack();
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            return msg.nack(e);
        }


//        CompletableFuture<Void> fut = new CompletableFuture<>();
//
//        processState(fut, msg, true);
//        fut.handle((res, ex) -> {
//            if (ex == null) {
//                msg.ack();
//            } else {
//                msg.nack(ex);
//            }
//            return null;
//        });
//        return CompletableFuture.completedFuture(null);
    }

    void processState(CompletableFuture<Void> fut, MqttMessage<byte[]> msg, boolean retry) {
        try {
            Matcher m = TOPIC_STATE_REGEX.matcher(msg.getTopic());
            if (!m.matches()) {
                LOG.warn("Can't parse state topic: {}", msg.getTopic());
                fut.complete(null);
            }
            String type = m.group(1);
            if (!qbusConfig.types().contains(type)) {
                LOG.debug("Ignoring state on type {} since not in config", type);
                fut.complete(null);
            }
            Integer id = Integer.parseInt(m.group(2));
            LOG.info("Qbus state on: {}", msg.getTopic());

            Map<Integer, Info> infoMap = info.get(type);
            if (infoMap == null) {
                LOG.warn("Can't find info type: {}, retry: {}", type, retry);
                if (retry) {
                    //racecondition: info topic might not be processed yet. Retry in a sec
                    vertx.setTimer(Duration.ofSeconds(1).toMillis(), x -> processState(fut, msg, false));
                } else {
                    fut.completeExceptionally(new IllegalStateException("can't find state"));
                }
                return;
            }
            Info info = infoMap.get(id);
            if (info == null) {
                LOG.warn("Can't find info for id: {}, type: {}, retry: {}", id, type, retry);
                if (retry) {
                    vertx.setTimer(Duration.ofSeconds(1).toMillis(), x -> processState(fut, msg, false));
                } else {
                    fut.completeExceptionally(new IllegalStateException("can't find info"));
                }
                return;
            }

            int data = Integer.parseInt(new String(msg.getPayload()));
            LOG.info("State update for \"{}\" to {}", info.name, data);
            values.get(type).put(id, data);
            fut.complete(null);
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.getTopic(), e);
            fut.completeExceptionally(e);
        }
    }

}
