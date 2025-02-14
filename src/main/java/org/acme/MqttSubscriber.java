package org.acme;

import io.vertx.mqtt.messages.MqttPublishMessage;

import java.util.List;

public interface MqttSubscriber {
    String getPrefix();

    List<String> getSubscriptions();

    void consume(MqttPublishMessage msg);
}
