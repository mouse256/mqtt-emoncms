package org.acme.wiremock;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.quarkus.test.common.QuarkusTestResourceConfigurableLifecycleManager;
import jakarta.ws.rs.core.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;

public class WiremockResourceConfigurable implements QuarkusTestResourceConfigurableLifecycleManager<WiremockTestAnnotation> {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static WireMockServer server;
    private String port;

    @Override
    public void init(WiremockTestAnnotation params) {
        port = params.port();
    }

    @Override
    public Map<String, String> start() {
        server = new WireMockServer(
                new WireMockConfiguration()
                        .port(Integer.parseInt(port))
                        .extensions(new StubResponseTransformerWithParams())
                );
        server.start();
        server.addMockServiceRequestListener((request, response) -> {
            LOG.info("Wiremock request: {} -- {}", request, response);
        });
//        server.loadMappingsUsing(mappings -> {
//            mappings.addMapping(new StubResponseTransformerWithParams());
//            mappings.
//        });

        ResponseDefinitionBuilder rdb = WireMock.aResponse()
                .withStatus(200)
                .withHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
                        .withTransformers("emoncms-input", "x", "y");
        server.stubFor(
                WireMock.post("/emoncms-mock/input/post").willReturn(rdb));


//        server.stubFor(
//                WireMock.get("/emoncms-mock/input/post").willReturn(WireMock.aResponse().withBody("{}")
//                        .withStatus(200).withHeader(HttpHeaders.CONTENT_TYPE, "application/json")));

        return Map.of(
                "emoncms.endpoint", "http://127.0.0.1:" + port + "/emoncms-mock",
                "emoncms.apikey", "xx",
                "emoncms.enabled", "true"
        );
    }

    @Override
    public void stop() {

    }

    public static class StubResponseTransformerWithParams implements ResponseTransformerV2 {

        @Override
        public Response transform(Response response, ServeEvent serveEvent) {
            LOG.info("transforming {}", serveEvent.getRequest().getBodyAsString());
            return Response.Builder.like(response)
                    //.configureDelay(null, null, 40_000, null)
                    .body("ok")
                    .build();
        }

        @Override
        public String getName() {
            return "emoncms-input";
        }
    }
}
