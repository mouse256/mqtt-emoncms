package org.acme;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import org.acme.wiremock.WiremockResourceConfigurable;
import org.acme.wiremock.WiremockTestAnnotation;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;


@QuarkusTest
//@TestHTTPEndpoint(WiremockResource.class)
@WiremockTestAnnotation(port = "57005")
public class MqttSubscriberSlimmelezerTest {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Inject
    @Channel("slimmelezertest")
    @OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
    MutinyEmitter<String> emitter;

    //@Inject
    //MqttReceiver receiver;


    @Test
    public void testMqttCommunication() throws InterruptedException {

        String topic1 = "slimmelezer/sensor/power_consumed_phase_1/state";
        //emitter.send(testMessage).await().indefinitely();

        WiremockResourceConfigurable.server.stubFor(
                WireMock.get("/imdb/film/1").willReturn(WireMock.aResponse().withBody("{}")
                        .withStatus(200).withHeader(HttpHeaders.CONTENT_TYPE, "application/json")));

        for (int i = 0; i < 20; i++) {
            LOG.info("Msg {}", i);
            String testMessage = Integer.toString(i);
            emitter.sendMessage(MqttMessage.of(topic1, testMessage)).await().indefinitely();
            if (i == 3) {
                for (int j = 0; j < 1000; j++) {
                    emitter.sendMessageAndForget(MqttMessage.of(topic1, testMessage));
                }
            }
            Thread.sleep(1_000);
        }

        Thread.sleep(120_000);
        // Wait for the message to be processed
        //String receivedMessage = receiver.getMessages().take();
        //assertEquals(testMessage, receivedMessage);
    }

    //@Inject
    //@ConfigProperty(name = "quarkus.rest-client.\"com.home.example.service.ExternalService\".url")
    //private String serverUrl;


}
