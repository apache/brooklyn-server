/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.rest.resources;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon.DslFormatString;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.rest.domain.ApplicationSpec;
import org.apache.brooklyn.rest.domain.EntityConfigSummary;
import org.apache.brooklyn.rest.domain.EntitySpec;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.rest.testing.mocks.RestMockSimpleEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "EntityConfigResourceTest")
public class EntityConfigResourceTest extends BrooklynRestResourceTest {
    
    private final static Logger log = LoggerFactory.getLogger(EntityConfigResourceTest.class);

    final static String HELLO_WORLD_DSL = "$brooklyn:formatString(\"%s-%s\", \"hello\", \"world\")";

    private URI applicationUri;
    private EntityInternal entity;

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        // Deploy an application that we'll use to read the configuration of
        final ApplicationSpec simpleSpec = ApplicationSpec.builder().name("simple-app").
                  entities(ImmutableSet.of(new EntitySpec("simple-ent", RestMockSimpleEntity.class.getName(), ImmutableMap.of("install.version", "1.0.0")))).
                  locations(ImmutableSet.of("localhost")).
                  build();

        startServer();
        Response response = clientDeploy(simpleSpec);
        int status = response.getStatus();
        assertTrue(status >= 200 && status <= 299, "expected HTTP Response of 2xx but got " + status);
        applicationUri = response.getLocation();
        log.debug("Built app: application");
        Application app = waitForApplicationToBeRunning(applicationUri);
        
        entity = (EntityInternal) Iterables.find(getManagementContext().getEntityManager().getEntities(), EntityPredicates.displayNameEqualTo("simple-ent"));
        Assert.assertEquals( Iterables.getOnlyElement(app.getChildren()), entity );

        entity.config().set( (ConfigKey) RestMockSimpleEntity.SECRET_CONFIG, new DslFormatString("%s-%s", "hello", "world") );
    }

    @Test
    public void testList() throws Exception {
        List<EntityConfigSummary> entityConfigSummaries = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/config"))
                .get(new GenericType<List<EntityConfigSummary>>() {
                });
        
        // Default entities have over a dozen config entries, but it's unnecessary to test them all; just pick one
        // representative config key
        Optional<EntityConfigSummary> configKeyOptional = Iterables.tryFind(entityConfigSummaries, new Predicate<EntityConfigSummary>() {
            @Override
            public boolean apply(@Nullable EntityConfigSummary input) {
                return input != null && "install.version".equals(input.getName());
            }
        });
        assertTrue(configKeyOptional.isPresent());
        
        assertEquals(configKeyOptional.get().getType(), "java.lang.String");
        assertEquals(configKeyOptional.get().getDescription(), "The suggested version of the software to be installed");
        assertFalse(configKeyOptional.get().isReconfigurable());
        assertNull(configKeyOptional.get().getDefaultValue());
        assertNull(configKeyOptional.get().getLabel());
        assertNull(configKeyOptional.get().getPriority());
    }

    @Test
    public void testBatchConfigRead() throws Exception {
        Map<String, Object> currentState = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/config/current-state"))
                .get(new GenericType<Map<String, Object>>() {
                });
        assertTrue(currentState.containsKey("install.version"));
        assertEquals(currentState.get("install.version"), "1.0.0");
    }

    @Test
    public void testGetJson() throws Exception {
        String configValue = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/config/install.version"))
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .get(String.class);
        assertEquals(configValue, "\"1.0.0\"");
    }

    @Test
    public void testGetPlain() throws Exception {
        String configValue = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/config/install.version"))
                .accept(MediaType.TEXT_PLAIN_TYPE)
                .get(String.class);
        assertEquals(configValue, "1.0.0");
    }

    @Test
    public void testGetJsonWithParameters() throws Exception {
        Function<String,String> getSecretWithQueryParams = qp ->
                client().path(
                        URI.create("/applications/simple-app/entities/simple-ent/config/"+RestMockSimpleEntity.SECRET_CONFIG.getName()))
                        .replaceQuery(qp)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .get(String.class);

        assertEquals(getSecretWithQueryParams.apply(""), JavaStringEscapes.wrapJavaString("hello-world"));
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true"), JavaStringEscapes.wrapJavaString("<suppressed> (MD5 hash: 20953121)"));
        assertEquals(getSecretWithQueryParams.apply("skipResolution=true"), JavaStringEscapes.wrapJavaString(HELLO_WORLD_DSL));
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), JavaStringEscapes.wrapJavaString(HELLO_WORLD_DSL));
    }

    @Test
    public void testGetBatchWithParameters() throws Exception {
        Function<String,Map> getSecretWithQueryParams1 = qp ->
                client().path(
                        URI.create("/applications/simple-app/entities/simple-ent/config/current-state"))
                        .replaceQuery(qp)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .get(Map.class);
        Function<String,String> getSecretWithQueryParams = qp ->
                Strings.toString(getSecretWithQueryParams1.apply(qp).get(RestMockSimpleEntity.SECRET_CONFIG.getName()));

        assertEquals(getSecretWithQueryParams.apply(""), "hello-world");
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true"), "<suppressed> (MD5 hash: 20953121)");
        assertEquals(getSecretWithQueryParams.apply("skipResolution=true"), HELLO_WORLD_DSL);
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), HELLO_WORLD_DSL);
    }

    @Test
    public void testGetJsonNestedSecret() throws Exception {
        MutableMap<String, Object> input = MutableMap.of("public", "visible", "secret", new DslFormatString("%s-%s", "hello", "world"));
        entity.config().set(ConfigKeys.newConfigKey(Object.class, "deep-see-cret"), input);

        Function<String,Map> getDeepSeecretWithQueryParams = qp ->
                client().path(
                        URI.create("/applications/simple-app/entities/simple-ent/config/deep-see-cret"))
                        .replaceQuery(qp)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .get(Map.class);

        assertEquals(getDeepSeecretWithQueryParams.apply(""), MutableMap.copyOf(input).add("secret", "hello-world"));
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true"), MutableMap.copyOf(input).add("secret", "<suppressed> (MD5 hash: 20953121)"));
        assertEquals(getDeepSeecretWithQueryParams.apply("skipResolution=true"), MutableMap.copyOf(input).add("secret", HELLO_WORLD_DSL));
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), MutableMap.copyOf(input).add("secret", HELLO_WORLD_DSL));
    }

    @Test
    public void testGetBatchNestedSecret() throws Exception {
        MutableMap<String, Object> input = MutableMap.of("public", "visible", "secret", new DslFormatString("%s-%s", "hello", "world"));
        ConfigKey<Object> DEEP_SEECRET = ConfigKeys.newConfigKey(Object.class, "deep-see-cret");
        entity.config().set(DEEP_SEECRET, input);

        Function<String,Map> getDeepSeecretWithQueryParams1 = qp ->
                client().path(
                        URI.create("/applications/simple-app/entities/simple-ent/config/current-state"))
                        .replaceQuery(qp)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .get(Map.class);

        Function<String,Map> getDeepSeecretWithQueryParams = qp ->
                (Map)getDeepSeecretWithQueryParams1.apply(qp).get(DEEP_SEECRET.getName());

        assertEquals(getDeepSeecretWithQueryParams.apply(""), MutableMap.copyOf(input).add("secret", "hello-world"));
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true"), MutableMap.copyOf(input).add("secret", "<suppressed> (MD5 hash: 20953121)"));
        assertEquals(getDeepSeecretWithQueryParams.apply("skipResolution=true"), MutableMap.copyOf(input).add("secret", HELLO_WORLD_DSL));
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), MutableMap.copyOf(input).add("secret", HELLO_WORLD_DSL));
    }

    @Test
    public void testGetPlainNestedSecret() throws Exception {
        MutableMap<String, Object> input = MutableMap.of("public", "visible", "secret", new DslFormatString("%s-%s", "hello", "world"));
        ConfigKey<Object> DEEP_SEECRET = ConfigKeys.newConfigKey(Object.class, "deep-see-cret");
        entity.config().set(DEEP_SEECRET, input);

        Function<String,String> getDeepSeecretWithQueryParams = qp ->
                client().path(
                        URI.create("/applications/simple-app/entities/simple-ent/config/deep-see-cret"))
                        .replaceQuery(qp)
                        .accept(MediaType.TEXT_PLAIN_TYPE)
                        .get(String.class);

        MutableMap<String, Object> resolved = MutableMap.copyOf(input).add("secret", "hello-world");
        MutableMap<String, Object> resolutionSkipped = MutableMap.copyOf(input).add("secret", HELLO_WORLD_DSL);

        assertEquals(getDeepSeecretWithQueryParams.apply(""), resolved.toString());
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true"), Sanitizer.suppress(new Gson().toJson(resolved)));
        assertEquals(getDeepSeecretWithQueryParams.apply("skipResolution=true"), resolutionSkipped.toString());
        assertEquals(getDeepSeecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), Sanitizer.suppress(new Gson().toJson(resolutionSkipped)));
    }

    @Test
    public void testGetPlainWithParameters() throws Exception {
        Function<String,String> getSecretWithQueryParams = qp ->
                client().path(
                    URI.create("/applications/simple-app/entities/simple-ent/config/"+RestMockSimpleEntity.SECRET_CONFIG.getName()))
                    .replaceQuery(qp)
                    .accept(MediaType.TEXT_PLAIN_TYPE)
                    .get(String.class);

        assertEquals(getSecretWithQueryParams.apply(""), "hello-world");
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true"), "<suppressed> (MD5 hash: 20953121)");
        assertEquals(getSecretWithQueryParams.apply("skipResolution=true"), HELLO_WORLD_DSL);
        assertEquals(getSecretWithQueryParams.apply("suppressSecrets=true&skipResolution=true"), HELLO_WORLD_DSL);
    }

    @Test
    public void testSet() throws Exception {
        try {
            String uri = "/applications/simple-app/entities/simple-ent/config/"+
                RestMockSimpleEntity.SAMPLE_CONFIG.getName();
            Response response = client().path(uri)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post("\"hello world\"");
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

            assertEquals(entity.getConfig(RestMockSimpleEntity.SAMPLE_CONFIG), "hello world");
            
            String value = client().path(uri).accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
            assertEquals(value, "\"hello world\"");

        } finally { entity.config().set(RestMockSimpleEntity.SAMPLE_CONFIG, RestMockSimpleEntity.SAMPLE_CONFIG.getDefaultValue()); }
    }

    @Test
    public void testSetFromMap() throws Exception {
        try {
            String uri = "/applications/simple-app/entities/simple-ent/config";
            Response response = client().path(uri)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(MutableMap.of(
                    RestMockSimpleEntity.SAMPLE_CONFIG.getName(), "hello world"));
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

            assertEquals(entity.getConfig(RestMockSimpleEntity.SAMPLE_CONFIG), "hello world");
            
            String value = client().path(uri+"/"+RestMockSimpleEntity.SAMPLE_CONFIG.getName()).accept(MediaType.APPLICATION_JSON_TYPE).get(String.class);
            assertEquals(value, "\"hello world\"");

        } finally { entity.config().set(RestMockSimpleEntity.SAMPLE_CONFIG, RestMockSimpleEntity.SAMPLE_CONFIG.getDefaultValue()); }
    }

}
