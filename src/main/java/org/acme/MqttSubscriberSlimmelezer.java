package org.acme;

import io.vertx.mqtt.messages.MqttPublishMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@ApplicationScoped
public class MqttSubscriberSlimmelezer implements MqttSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final SlimmelezerConfig slimmelezerConfig;
    private static final String PREFIX = "slimmelezer/sensor/";
    private static final Pattern PROPERTIES_REGEX = Pattern.compile("^" + PREFIX + "(\\S+)/state$");
    private final EmonPosterCache emonPoster;
    private static final String DEVICE = "slimmelezer";


    public MqttSubscriberSlimmelezer(SlimmelezerConfig slimmelezerConfig, EmonPosterCache emonPoster) {
        this.slimmelezerConfig = slimmelezerConfig;
        this.emonPoster = emonPoster;
        emonPoster.setName("Slimmelezer");
    }

    //    @Incoming("qbusState")
    //    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    //    CompletionStage<Void> consumeState(MqttMessage<byte[]> msg) {

    public void consume(MqttPublishMessage msg) {
        if (!slimmelezerConfig.enabled()) {
            LOG.debug("Slimmelezer is disabled");
        }
        try {
            Thread.sleep(1);
            LOG.debug("Incoming message on: {}", msg.topicName());
            Matcher matcher = PROPERTIES_REGEX.matcher(msg.topicName());
            if (matcher.matches()) {
                String meterName = matcher.group(1);
                String meterConfig = slimmelezerConfig.items().get(meterName);
                if (meterConfig == null) {
                    LOG.debug("No input handled for this meter: {}: ({})", meterName, msg.topicName());
                    return;
                }
                double value = Double.parseDouble(new String(msg.payload().getBytes()));
                emonPoster.add(DEVICE, meterConfig, value);
            } else {
                LOG.debug("Don't know how to handle topic {}", msg.topicName());
            }
        } catch (Exception e) {
            LOG.warn("Could not parse message on topic {}", msg.topicName(), e);
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
}
