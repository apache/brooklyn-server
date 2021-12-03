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
package org.apache.brooklyn.camp.brooklyn.rebind;

import com.google.common.collect.BiMap;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import java.io.File;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RebindMiscTest extends AbstractYamlRebindTest {

    // TODO What about testing DslBrooklynObjectConfigSupplier?
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RebindMiscTest.class);

    // update of Guava removed BiMap class, we have to handle it ourselves
    @Test
    public void testGuavaEmptyBiMap_2021_12() throws Exception {
        String entityId = "hrxo4j0dcs";
        doAddEntityMemento("guava-2021-12", entityId);
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(entityId);

        BiMap diags = newEntity.sensors().get(Sensors.newSensor(BiMap.class, "service.notUp.diagnostics"));
        assertEquals(diags.size(), 0);
    }

    protected void doAddEntityMemento(String label, String entityId) throws Exception {
        String mementoResourceName = "misc-" + label + "-entity-" + entityId;
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoResourceName));
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(memento.getBytes(), persistedEntityFile);
    }
}
