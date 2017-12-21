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
package org.apache.brooklyn.core.mgmt.ha;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OsgiArchiveInstallerTest extends BrooklynMgmtUnitTestSupport {

    // The tests here will so far not need an actual OSGi Framework! Therefore we're using the simple
    // BrooklynMgmtUnitTestSupport, which does not expose `useOsgi` or `osgiReuse`
    
    @Test
    public void testBlacklistPersistingOrgApacheBrooklyn() throws Exception {
        OsgiManager osgiManager = newMockOsgiManager(mgmt);
        OsgiArchiveInstaller installer = new OsgiArchiveInstaller(osgiManager, Mockito.mock(ManagedBundle.class), new ByteArrayInputStream(new byte[0]));
        
        assertTrue(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.brooklyn.core", "1.0.0")));
        assertTrue(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.brooklyn.mybundle", "1.0.0")));
        assertFalse(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.different", "1.0.0")));
    }

    @Test
    public void testWhitelistPersistingBundle() throws Exception {
        mgmt.getBrooklynProperties().put(BrooklynServerConfig.PERSIST_MANAGED_BUNDLE_WHITELIST_REGEX, "org\\.apache\\.brooklyn\\.mywhitelistedbundle");
        OsgiManager osgiManager = newMockOsgiManager(mgmt);
        OsgiArchiveInstaller installer = new OsgiArchiveInstaller(osgiManager, Mockito.mock(ManagedBundle.class), new ByteArrayInputStream(new byte[0]));
        
        assertTrue(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.brooklyn.core", "1.0.0")));
        assertFalse(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.brooklyn.mywhitelistedbundle", "1.0.0")));
    }

    @Test
    public void testCustomBlacklistPersistingBundle() throws Exception {
        mgmt.getBrooklynProperties().put(BrooklynServerConfig.PERSIST_MANAGED_BUNDLE_BLACKLIST_REGEX, "org\\.example\\.myblacklistprefix.*");
        OsgiManager osgiManager = newMockOsgiManager(mgmt);
        OsgiArchiveInstaller installer = new OsgiArchiveInstaller(osgiManager, Mockito.mock(ManagedBundle.class), new ByteArrayInputStream(new byte[0]));
        
        assertTrue(installer.isBlacklistedForPersistence(newMockManagedBundle("org.example.myblacklistprefix.mysuffix", "1.0.0")));
        assertFalse(installer.isBlacklistedForPersistence(newMockManagedBundle("org.apache.brooklyn.core", "1.0.0")));
    }

    @Test
    public void testInferBundleNameFromMvnUrl() throws Exception {
        assertFalse(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:toofewslashes/1.0.0").isPresent());
        assertFalse(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:too/many/slashes/1.0.0").isPresent());
        assertFalse(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:emptystring//1.0.0").isPresent());
        assertFalse(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:/emptystring/1.0.0").isPresent());
        assertFalse(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:/emptystring/emptystring/").isPresent());
        assertEquals(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:mygroupid/myartifactid/1.0.0").get(), new VersionedName("mygroupid.myartifactid", "1.0.0"));
        assertEquals(OsgiArchiveInstaller.inferBundleNameFromMvnUrl("mvn:my.group.id/my.artifact.id/1.0.0").get(), new VersionedName("my.group.id.my.artifact.id", "1.0.0"));
    }
    
    public OsgiManager newMockOsgiManager(ManagementContext mgmt) throws Exception {
        OsgiManager result = Mockito.mock(OsgiManager.class);
        Mockito.when(result.getManagementContext()).thenReturn(mgmt);
        return result;
    }
    
    private ManagedBundle newMockManagedBundle(String symbolicName, String version) {
        VersionedName versionedName = new VersionedName(symbolicName, version);
        ManagedBundle result = Mockito.mock(ManagedBundle.class);
        Mockito.when(result.getSymbolicName()).thenReturn(symbolicName);
        Mockito.when(result.getOsgiVersionString()).thenReturn(versionedName.getOsgiVersionString());
        Mockito.when(result.getVersionedName()).thenReturn(versionedName);
        return result;
    }
}
