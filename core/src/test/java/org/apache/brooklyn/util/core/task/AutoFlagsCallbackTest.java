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
package org.apache.brooklyn.util.core.task;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class AutoFlagsCallbackTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(AutoFlagsCallbackTest.class);

    @Override
    protected void setUpApp() {
        addDecoratorForTasks();
        super.setUpApp();
    }

    /*
        Once at circle CI seen this do the following:

        2026-03-04 22:25:28,687 INFO  o.a.b.u.c.t.AutoFlagsCallbackTest                 STARTING TASK: Task[test1]@C4ufGPP9
        2026-03-04 22:25:28,687 INFO  o.a.b.u.c.t.AutoFlagsCallbackTest                 running test 1 / C4ufGPP9
        2026-03-04 22:25:28,687 INFO  o.a.b.u.c.t.AutoFlagsCallbackTest                 STARTING TASK: Task[sensor o8irhpj71e:service.isUp false]@JAksGu5s
        2026-03-04 22:25:28,687 INFO  o.a.b.u.c.t.AutoFlagsCallbackTest                 ENDING TASK: Task[sensor o8irhpj71e:service.isUp false]@JAksGu5s
        2026-03-04 22:25:28,687 INFO  o.a.b.u.c.t.AutoFlagsCallbackTest                 ENDING TASK: Task[test1]@C4ufGPP9
*/
    AtomicInteger depth = new AtomicInteger(0);

    @Test
    public void testCalledInOrder() {
        // might still be settling sensors
        Asserts.assertThat(depth.get(), x -> x>=0);

        app.start(null);
        // sensor tasks triggered during start may complete slightly after start() returns; wait for them
        Asserts.eventually(() -> depth.get(), x -> x == 0);

        Entities.submit(app, Tasks.create("test1", () -> {
            log.info("running test 1" + " / " + Tasks.current().getId());
            Asserts.assertEquals(depth.get(), 1);
        })).getUnchecked();

        Asserts.assertEquals(depth.get(), 0);
    }

    private void addDecoratorForTasks() {
        ((BasicExecutionManager) mgmt.getExecutionManager()).getAutoFlagsLive().put(BasicExecutionManager.TASK_START_CALLBACK_TAG, (Runnable) () -> {
            if (Tasks.isNonProxyTask()) {
                // above necessary to prevent this running from
                log.info("STARTING TASK: " + Tasks.current());
                Asserts.assertThat(depth.incrementAndGet(), x -> x>0);
            } else {
                // wrapper task eg DST
            }
        });
        ((BasicExecutionManager) mgmt.getExecutionManager()).getAutoFlagsLive().put(BasicExecutionManager.TASK_END_CALLBACK_TAG, (Runnable) () -> {
            if (Tasks.isNonProxyTask()) {
                log.info("ENDING TASK: " + Tasks.current());
                Asserts.assertThat(depth.decrementAndGet(), x -> x>=0);
            } else {
                // wrapper task eg DST
            }
        });
    }

}
