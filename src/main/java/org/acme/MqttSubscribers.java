package org.acme;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;

@ApplicationScoped
public class MqttSubscribers {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final Vertx vertx;
    private MqttClient mqttClient;
    private volatile boolean started = false;
    private MqttConfig mqttConfig;

    @Inject
    @All
    List<MqttSubscriber> subscribers;

    public MqttSubscribers(Vertx vertx, MqttConfig mqttConfig) {
        this.vertx = vertx;
        this.mqttConfig = mqttConfig;
    }

    public void onStart(@Observes StartupEvent startupEvent) {
        start();
    }

    private void start() {
        LOG.info("Verticle starting");
        if (!mqttConfig.enabled()) {
            LOG.warn("MQTT not enabled");
            return;
        }
        MqttClientOptions mqttClientOptions = new MqttClientOptions()
                .setMaxInflightQueue(200);
        mqttClient = MqttClient.create(vertx, mqttClientOptions);

        connectMqtt(() -> {
            LOG.info("MQTT ready");
            started = true;
            subscribe();
        });
        mqttClient.closeHandler(v -> {
            LOG.info("Mqtt closed, restart");
            restart();
        });
        mqttClient.exceptionHandler(ex -> {
            LOG.warn("Exception", ex);
            restart();
        });
    }

    private void connectMqtt(Runnable onConnected) {
        mqttClient.connect(mqttConfig.port(), mqttConfig.host(), ar -> {
            if (ar.failed()) {
                LOG.warn("MQTT connection failed, retrying in 60 s", ar.cause());
                vertx.setTimer(Duration.ofSeconds(60).toMillis(), l -> {
                    connectMqtt(onConnected);
                });
            } else {
                LOG.info("MQTT connected");
                onConnected.run();
            }
        });
    }

    public void stop(@Observes ShutdownEvent shutdownEvent) {
        stop();
    }

    private void stop() {
        LOG.info("Stopping");
        started = false;
        //consumer.unregister()
        if (mqttClient.isConnected()) {
            mqttClient.disconnect();
        }
    }

    private void restart() {
        if (!started) {
            LOG.warn("Cannot restart, not yet running");
            return;
        }
        stop();
        LOG.info("Restarting in 30s");
        vertx.setTimer(Duration.ofSeconds(30).toMillis(), l -> {
            start();
        });
    }

    private void subscribe() {
        mqttClient.publishHandler(this::handleMsg);
        subscribers.forEach(subscriber -> {
            subscriber.getSubscriptions().forEach(topic -> {
                LOG.info("Subscribing to topic {}", topic);
                mqttClient.subscribe(
                        topic,
                        MqttQoS.AT_MOST_ONCE.value()
                );
            });
        });
    }

    private void handleMsg(MqttPublishMessage msg) {
        handleMsgWitchAck(msg);
        msg.ack();
    }

    private void handleMsgWitchAck(MqttPublishMessage msg) {
        LOG.debug("Got msg on {}", msg.topicName());

        for (MqttSubscriber subscriber : subscribers) {
            if (msg.topicName().startsWith(subscriber.getPrefix())) {
                subscriber.consume(msg);
                return;
            }
        }
        LOG.warn("No subscriber found for {}", msg.topicName());
    }
}
