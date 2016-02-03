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
package org.apache.brooklyn.cm.salt;

import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.cm.salt.impl.SaltHighstate;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Highstate utility.
 */
public class HighstateTest {

    private static final Logger LOG = LoggerFactory.getLogger(HighstateTest.class);

    private TestApplication app = null;
    private SaltEntity entity = null;

    @AfterMethod(alwaysRun=true)
    public void tearDown() {
        if ( app != null) {
            Entities.destroyAll(app.getManagementContext());
            app = null;
        }
    }

    @Test
    public void shouldSetSensorsOnEntity() throws Exception {
        String contents = getTestYaml();
        TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(SaltEntity.class)
            .configure(SaltEntity.START_STATES, ImmutableSet.of("apache")));

        SaltHighstate.applyHighstate(contents, entity);

        final List<String> states = entity.sensors().get(SaltEntity.STATES);
        assertThat(states)
            .contains("apache")
            .contains("apache-reload")
            .contains("apache-restart");

        final Map<String, Object> apachePkgInstalled =
            entity.sensors().get(Sensors.newSensor(SaltHighstate.STATE_FUNCTION_TYPE, "salt.state.apache.pkg.installed", ""));
        assertThat(apachePkgInstalled).isNotNull();
        assertThat(apachePkgInstalled.get("name")).isEqualTo("apache2");
        assertThat(apachePkgInstalled.get("order")).isEqualTo(10000);

        final Map<String, Object> apacheServiceRunning =
            entity.sensors().get(Sensors.newSensor(SaltHighstate.STATE_FUNCTION_TYPE, "salt.state.apache.service.running", ""));
        assertThat(apacheServiceRunning).isNotNull();
        assertThat(apacheServiceRunning.get("name")).isEqualTo("apache2");
        assertThat(apacheServiceRunning.get("order")).isEqualTo(10001);
        assertThat(apacheServiceRunning.get("enable"));
    }


    private String getTestYaml() {
        final ResourceUtils resourceUtils = ResourceUtils.create();
        final InputStream yaml = resourceUtils.getResourceFromUrl("classpath://test-highstate.yaml");
        return Streams.readFullyString(yaml);
    }
}
