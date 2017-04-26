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

import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RebindHistoricSshFeedTest extends RebindAbstractCommandFeedTest {
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
}
