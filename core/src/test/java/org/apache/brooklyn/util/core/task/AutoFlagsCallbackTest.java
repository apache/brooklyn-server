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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicReference;

public class AutoFlagsCallbackTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(AutoFlagsCallbackTest.class);

    @Test
    public void testCalledInOrder() {
        AtomicReference<String> state = new AtomicReference<>("end");
        ((BasicExecutionManager) mgmt.getExecutionManager()).getAutoFlagsLive().put(BasicExecutionManager.TASK_START_CALLBACK_TAG, (Runnable) () -> {
            if (Tasks.isNonProxyTask()) {
                // above necessary to prevent this running from
                log.info("STARTING TASK: " + Tasks.current());
                Asserts.assertTrue(state.compareAndSet("end", "start"));
            } else {
                // wrapper task eg DST
            }
        });
        ((BasicExecutionManager) mgmt.getExecutionManager()).getAutoFlagsLive().put(BasicExecutionManager.TASK_END_CALLBACK_TAG, (Runnable) () -> {
            if (Tasks.isNonProxyTask()) {
                state.compareAndSet("start-callback", "main");
                log.info("ENDING TASK: " + Tasks.current());
                Asserts.assertTrue(state.compareAndSet("start", "end"));
            } else {
                // wrapper task eg DST
            }
        });
        Entities.submit(app, Tasks.create("test1", () -> {
            log.info("running test 1" + " / " + Tasks.current().getId());
            Asserts.assertEquals(state.get(), "start");
        })).getUnchecked();

        Asserts.assertEquals(state.get(), "end");
    }

}
