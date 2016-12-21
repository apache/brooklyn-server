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
package org.apache.brooklyn.core.effector.http;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;

public class HttpCommandEffectorIntegrationTest {

    final static Effector<String> EFFECTOR_GITHUB_APACHE_ACCOUNT = Effectors.effector(String.class, "GithubApacheAccount").buildAbstract();

    private TestApplication app;
    private EntityLocal entity;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class).location(TestApplication.LOCALHOST_MACHINE_SPEC));
        app.start(ImmutableList.<Location>of());
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());
    }

    @Test(groups="Integration")
    public void testHttpEffector() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "GithubApacheAccount")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/users/apache")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
        ).apply(entity);

        String val = entity.invoke(EFFECTOR_GITHUB_APACHE_ACCOUNT, MutableMap.<String,String>of()).get();
        Assert.assertEquals(JsonPath.parse(val).read("$.login", String.class), "apache");
    }

    @Test(groups="Integration")
    public void testHttpEffectorWithPayload() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "CreateGist")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/gists")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, ImmutableMap.<String, Object>of(
                        "description", "Created via API", 
                        "public", "false",
                        "files", ImmutableMap.of("demo.txt", ImmutableMap.of("content","Demo"))))
                .configure(HttpCommandEffector.EFFECTOR_HTTP_HEADERS, ImmutableMap.of("Content-Type", "application/json"))
                .configure(HttpCommandEffector.JSON_PATH, "$.url")
                .configure(HttpCommandEffector.PUBLISH_SENSOR, "result")
        ).apply(entity);

        String url = entity.invoke(Effectors.effector(String.class, "CreateGist").buildAbstract(), MutableMap.<String,String>of()).get();
        Assert.assertNotNull(url, "url");
    }

    @Test(groups="Integration")
    public void testHttpEffectorWithJsonPath() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "GithubApacheAccount")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/users/apache")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
                .configure(HttpCommandEffector.JSON_PATH, "$.login")
                .configure(HttpCommandEffector.PUBLISH_SENSOR, "result")
        ).apply(entity);

        String val = entity.invoke(EFFECTOR_GITHUB_APACHE_ACCOUNT, MutableMap.<String,String>of()).get();
        Assert.assertEquals(val, "apache");
        Assert.assertEquals(entity.sensors().get(Sensors.newStringSensor("result")), "apache");
    }
    
    @Test(groups="Integration")
    public void testHttpEffectorWithParameters() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "GithubApacheAccount")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/users/$user")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
                .configure(HttpCommandEffector.EFFECTOR_PARAMETER_DEFS,
                        MutableMap.<String,Object>of("user", MutableMap.of("defaultValue", "apache"))))
                .apply(entity);

        String val;
        // explicit value
        val = entity.invoke(EFFECTOR_GITHUB_APACHE_ACCOUNT, MutableMap.of("user", "github")).get();
        Assert.assertEquals(JsonPath.parse(val).read("$.login", String.class), "github");

        // default value
        val = entity.invoke(EFFECTOR_GITHUB_APACHE_ACCOUNT, MutableMap.<String,String>of()).get();
        Assert.assertEquals(JsonPath.parse(val).read("$.login", String.class), "apache");
    }
}
