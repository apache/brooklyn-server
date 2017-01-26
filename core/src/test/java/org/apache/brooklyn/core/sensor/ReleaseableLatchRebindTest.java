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
package org.apache.brooklyn.core.sensor;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

public class ReleaseableLatchRebindTest extends RebindTestFixtureWithApp {

    @Test(timeOut = Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    public void testRebindResetsPermits() throws Exception {
        final AttributeSensor<ReleaseableLatch> latchSensor = Sensors.newSensor(ReleaseableLatch.class, "latch");
        final ReleaseableLatch latchSemaphore = ReleaseableLatch.Factory.newMaxConcurrencyLatch(1);
        origApp.sensors().set(latchSensor, latchSemaphore);
        latchSemaphore.acquire(origApp);
 
        rebind();

        ReleaseableLatch newSemaphore = newApp.sensors().get(latchSensor);
        // makes sure permits are reset and we can acquire the semaphore again
        newSemaphore.acquire(origApp);
    }

}
