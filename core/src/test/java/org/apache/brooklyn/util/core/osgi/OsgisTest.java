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
package org.apache.brooklyn.util.core.osgi;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.mockito.Mockito;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Version;
import org.osgi.framework.launch.Framework;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class OsgisTest {

    private Framework framework;
    private BundleContext bundleContext;
    private Bundle myBundle1_0_0;
    private Bundle myBundle1_1_0;
    private Bundle myBundle2_0_0;
    private Bundle myBundle2_0_0_snapshot;
    private Bundle otherBundle1_0_0;
    private Bundle snapshotBundle1_0_0_snapshot;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        myBundle1_0_0 = Mockito.mock(Bundle.class);
        Mockito.when(myBundle1_0_0.getSymbolicName()).thenReturn("mybundle");
        Mockito.when(myBundle1_0_0.getVersion()).thenReturn(Version.parseVersion("1.0.0"));
        
        myBundle1_1_0 = Mockito.mock(Bundle.class);
        Mockito.when(myBundle1_1_0.getSymbolicName()).thenReturn("mybundle");
        Mockito.when(myBundle1_1_0.getVersion()).thenReturn(Version.parseVersion("1.1.0"));
        
        myBundle2_0_0 = Mockito.mock(Bundle.class);
        Mockito.when(myBundle2_0_0.getSymbolicName()).thenReturn("mybundle");
        Mockito.when(myBundle2_0_0.getVersion()).thenReturn(Version.parseVersion("2.0.0"));
        
        myBundle2_0_0_snapshot = Mockito.mock(Bundle.class);
        Mockito.when(myBundle2_0_0_snapshot.getSymbolicName()).thenReturn("mybundle");
        Mockito.when(myBundle2_0_0_snapshot.getVersion()).thenReturn(Version.parseVersion("2.0.0.SNAPSHOT"));
        
        otherBundle1_0_0 = Mockito.mock(Bundle.class);
        Mockito.when(otherBundle1_0_0.getSymbolicName()).thenReturn("otherbundle");
        Mockito.when(otherBundle1_0_0.getVersion()).thenReturn(Version.parseVersion("1.0.0"));
        
        snapshotBundle1_0_0_snapshot = Mockito.mock(Bundle.class);
        Mockito.when(snapshotBundle1_0_0_snapshot.getSymbolicName()).thenReturn("snapshotbundle");
        Mockito.when(snapshotBundle1_0_0_snapshot.getVersion()).thenReturn(Version.parseVersion("1.0.0.SNAPSHOT"));
        
        bundleContext = Mockito.mock(BundleContext.class);
        Mockito.when(bundleContext.getBundles()).thenReturn(new Bundle[] {myBundle1_0_0, myBundle1_1_0, myBundle2_0_0, 
                myBundle2_0_0_snapshot, otherBundle1_0_0, snapshotBundle1_0_0_snapshot});
        
        framework = Mockito.mock(Framework.class);
        Mockito.when(framework.getBundleContext()).thenReturn(bundleContext);
        
    }
    
    @Test
    public void testFindByNameAndVersion() throws Exception {
        Bundle result = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("1.0.0")
                .find()
                .get();
        assertEquals(result, myBundle1_0_0);
    }

    @Test
    public void testFindAllByNameAndVersion() throws Exception {
        List<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("1.0.0")
                .findAll();
        assertEquals(result, ImmutableList.of(myBundle1_0_0));
    }
    
    @Test
    public void testFindByNameAndVersionRange() throws Exception {
        List<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("[1.0.0, 1.1.0)")
                .findAll();
        assertEquals(result, ImmutableList.of(myBundle1_0_0));
        
        List<Bundle> result2 = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("[1.0.0, 2.0.0)")
                .findAll();
        assertEquals(result2, ImmutableList.of(myBundle1_0_0, myBundle1_1_0));
        
        List<Bundle> result3 = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("[1.0.0, 2.0.0]")
                .findAll();
        assertEquals(result3, ImmutableList.of(myBundle1_0_0, myBundle1_1_0, myBundle2_0_0));
        
        List<Bundle> result4 = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .version("[2.0.0, 3.0.0)")
                .findAll();
        assertEquals(result4, ImmutableList.of(myBundle2_0_0_snapshot, myBundle2_0_0));
        
        List<Bundle> result5 = Osgis.bundleFinder(framework)
                .symbolicName("snapshotbundle")
                .version("[1.0.0, 2.0.0)")
                .findAll();
        assertEquals(result5, ImmutableList.of(snapshotBundle1_0_0_snapshot));
    }

    @Test
    public void testFindAllByNameOnly() throws Exception {
        List<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName("mybundle")
                .findAll();
        assertEquals(result, ImmutableList.of(myBundle2_0_0_snapshot, myBundle1_0_0, myBundle1_1_0, myBundle2_0_0));
    }
}
