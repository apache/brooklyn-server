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
package org.apache.brooklyn.camp.brooklyn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.util.time.Duration;

@Test
public class EffectorsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(EffectorsYamlTest.class);

    @Test
    public void testWithAppEnricher() throws Exception {
        Entity app = createAndStartApplication(loadYaml("test-app-with-effectors.yaml"));
        waitForApplicationTasks(app);
        Assert.assertEquals(app.getDisplayName(), "test-app-with-effectors");

        Entities.dumpInfo(app);
        Thread.sleep(Duration.THIRTY_SECONDS.toMilliseconds());

        Entity start1 = null, start2 = null;
        Assert.assertEquals(app.getChildren().size(), 2);
        for (Entity child : app.getChildren()) {
            if (child.getDisplayName().equals("start1"))
                start1 = child;
            if (child.getDisplayName().equals("start2"))
                start2 = child;
        }
        Assert.assertNotNull(start1);
        Assert.assertNotNull(start2);
        EntityAsserts.assertAttributeEquals(start1, Startable.SERVICE_UP, false);
        EntityAsserts.assertAttributeEquals(start2, Startable.SERVICE_UP, true);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
