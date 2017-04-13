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
package org.apache.brooklyn.core.mgmt.rebind;

import java.io.File;

import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager.RebindFailureMode;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

public class RebindPolicyPrivateConstructorTest extends RebindTestFixtureWithApp {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RebindPolicyPrivateConstructorTest.class);

    @SuppressWarnings("unused")
    private static class PrivatePolicy extends AbstractPolicy {
        private PrivatePolicy() {
        }
    }
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-474
    // 
    // However, instead of using ZooKeeperEnsembleImpl$MemberTrackingPolicy (which would require 
    // being in `brooklyn-library` repo), we use a test policy written in a similar way - i.e. it
    // has a private no-arg construtor.
    @Test
    public void testPolicyWithPrivateConstructor() throws Exception {
        addMemento(BrooklynObjectType.POLICY, "policy-private-no-arg-constructor", "lplpmv3goo");
        rebind();
    }
    
    @Override
    protected TestApplication rebind() throws Exception {
        RebindExceptionHandler exceptionHandler = RebindExceptionHandlerImpl.builder()
                .danglingRefFailureMode(RebindFailureMode.FAIL_AT_END)
                .rebindFailureMode(RebindFailureMode.FAIL_AT_END)
                .addConfigFailureMode(RebindFailureMode.FAIL_AT_END)
                .addPolicyFailureMode(RebindFailureMode.FAIL_AT_END)
                .loadPolicyFailureMode(RebindFailureMode.FAIL_AT_END)
                .build();
        return super.rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
    }
    
    protected void addMemento(BrooklynObjectType type, String label, String id) throws Exception {
        String mementoFilename = label+"-"+id;
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoFilename));
        
        File persistedFile = getPersistanceFile(type, id);
        Files.write(memento.getBytes(), persistedFile);
    }
    
    protected File getPersistanceFile(BrooklynObjectType type, String id) {
        String dir;
        switch (type) {
            case POLICY: dir = "policies"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(mementoDir, Os.mergePaths(dir, id));
    }
}
