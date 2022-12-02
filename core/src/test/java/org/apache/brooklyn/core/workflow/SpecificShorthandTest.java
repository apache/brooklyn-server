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
package org.apache.brooklyn.core.workflow;

import org.apache.brooklyn.core.workflow.steps.flow.RetryWorkflowStep.RetryLimit;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

public class SpecificShorthandTest {

    protected void assertRetry(String input, Integer count, Duration duration) {
        RetryLimit x = RetryLimit.fromString(input);
        Asserts.assertEquals(x.count, count);
        Asserts.assertEquals(x.duration, duration);
    }

    @Test
    public void testRetryLimit() {
        assertRetry("5", 5, null);
        assertRetry("5m", null, Duration.minutes(5));
        assertRetry("2 in 5m", 2, Duration.minutes(5));
    }

}
