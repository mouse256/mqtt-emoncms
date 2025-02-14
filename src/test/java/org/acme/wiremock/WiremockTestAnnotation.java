package org.acme.wiremock;

import io.quarkus.test.common.QuarkusTestResource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@QuarkusTestResource(value = WiremockResourceConfigurable.class, restrictToAnnotatedClass = true)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WiremockTestAnnotation {
    String port() default "57005";
}
