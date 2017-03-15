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
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;

public class RebindHistoricSshFeedTest extends RebindTestFixtureWithApp {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RebindHistoricSshFeedTest.class);

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }
        
    // The persisted state contains renamed classes, such as:
    //   org.apache.brooklyn.feed.ssh.SshFeed$SshPollIdentifier : org.apache.brooklyn.feed.AbstractCommandFeed$CommandPollIdentifier
    //
    // These classnames include the bundle prefix (e.g. "org.apache.brooklyn.core:").
    // Prior to 2017-01-20 (commit dfd4315565c6767ccb16979c8d098717ed2a4853), classes in whitelisted
    // bundles would not include this prefix.
    @Test
    public void testSshFeed_2017_01() throws Exception {
        addMemento(BrooklynObjectType.FEED, "ssh-feed", "zv7t8bim62");
        rebind();
    }
    
    // This test is similar to testSshFeed_2017_01, except the persisted state file has been 
    // hand-crafted to remove the bundle prefixes for "org.apache.brooklyn.*" bundles.
    @Test
    public void testFoo_2017_01_withoutBundlePrefixes() throws Exception {
        addMemento(BrooklynObjectType.FEED, "ssh-feed-no-bundle-prefixes", "zv7t8bim62");
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
            case FEED: dir = "feeds"; break;
            default: throw new UnsupportedOperationException("type="+type);
        }
        return new File(mementoDir, Os.mergePaths(dir, id));
    }
}
