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
package org.apache.brooklyn.core.entity;

import org.testng.annotations.Test;

@Test(invocationCount=100, groups="Integration")
public class ApplicationLifecycleStateStressTest extends ApplicationLifecycleStateTest {
    
    // TODO Is there a nicer way to get all the super-class public methods to run with the 
    // invocationCount defined on this class, rather than the simple `@Test` annotation on 
    // the super-class (without redefining all the public methods here!)?
    
    @Override
    public void testHappyPathEmptyApp() throws Exception {
        super.testHappyPathEmptyApp();
    }
    
    @Override
    public void testHappyPathWithChild() throws Exception {
        super.testHappyPathWithChild();
    }
    
    @Override
    public void testOnlyChildFailsToStartCausesAppToFail() throws Exception {
        super.testOnlyChildFailsToStartCausesAppToFail();
    }
    
    @Override
    public void testSomeChildFailsOnStartCausesAppToFail() throws Exception {
        super.testSomeChildFailsOnStartCausesAppToFail();
    }
    
    @Override
    public void testOnlyChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        super.testOnlyChildFailsToStartThenRecoversCausesAppToRecover();
    }
    
    @Override
    public void testSomeChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        super.testSomeChildFailsToStartThenRecoversCausesAppToRecover();
    }
    
    @Override
    public void testStartsThenOnlyChildFailsCausesAppToFail() throws Exception {
        super.testStartsThenOnlyChildFailsCausesAppToFail();
    }

    @Override
    public void testStartsThenSomeChildFailsCausesAppToFail() throws Exception {
        super.testStartsThenSomeChildFailsCausesAppToFail();
    }

    /** See {@link ApplicationLifecycleStateTest#testChildFailuresOnStartButWithQuorumCausesAppToSucceed()} for details on the failure */
    @Test(groups="Broken")
    @Override
    public void testChildFailuresOnStartButWithQuorumCausesAppToSucceed() throws Exception {
        super.testChildFailuresOnStartButWithQuorumCausesAppToSucceed();
    }
    
    /** See {@link ApplicationLifecycleStateTest#testChildFailuresOnStartButWithQuorumCausesAppToSucceed()} for details on the failure */
    @Test(groups="Broken")
    @Override
    public void testStartsThenChildFailsButWithQuorumCausesAppToSucceed() throws Exception {
        super.testStartsThenChildFailsButWithQuorumCausesAppToSucceed();
    }

    /** See {@link ApplicationLifecycleStateTest#testChildFailuresOnStartButWithQuorumCausesAppToSucceed()} for details on the failure */
    @Test(groups="Broken")
    @Override
    public void testStartsThenChildFailsButWithQuorumCausesAppToStayHealthy() throws Exception {
        super.testStartsThenChildFailsButWithQuorumCausesAppToStayHealthy();
    }
}
