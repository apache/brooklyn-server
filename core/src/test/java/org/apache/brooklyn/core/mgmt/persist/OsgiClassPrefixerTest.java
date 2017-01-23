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
package org.apache.brooklyn.core.mgmt.persist;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.util.osgi.OsgiUtils;
import org.mockito.Mockito;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public class OsgiClassPrefixerTest {

    @Test
    public void testGetPrefixFromNonBundle() throws Exception {
        OsgiClassPrefixer prefixer = new OsgiClassPrefixer();
        assertAbsent(prefixer.getPrefix(String.class));
    }
    
    @Test
    public void testGetPrefixWithBundle() throws Exception {
        final Class<?> classInBundle = String.class;
        
        final Bundle bundle = Mockito.mock(Bundle.class);
        Mockito.when(bundle.getSymbolicName()).thenReturn("my.symbolic.name");
        Mockito.when(bundle.getVersion()).thenReturn(Version.valueOf("1.2.3"));
        
        Function<Class<?>, Optional<Bundle>> bundleRetriever = new Function<Class<?>, Optional<Bundle>>() {
            @Override public Optional<Bundle> apply(Class<?> input) {
                return (classInBundle.equals(input)) ? Optional.of(bundle) : Optional.<Bundle>absent();
            }
        };
        OsgiClassPrefixer prefixer = new OsgiClassPrefixer(bundleRetriever);
        assertAbsent(prefixer.getPrefix(Number.class));
        assertPresent(prefixer.getPrefix(String.class), "my.symbolic.name:");
    }
    
    @Test
    public void testGetPrefixWithWhitelistedBundle() throws Exception {
        final Bundle bundle = Mockito.mock(Bundle.class);
        Mockito.when(bundle.getSymbolicName()).thenReturn("org.apache.brooklyn.my-bundle");
        Mockito.when(bundle.getVersion()).thenReturn(Version.valueOf(OsgiUtils.toOsgiVersion(BrooklynVersion.get())));
        
        Function<Class<?>, Optional<Bundle>> bundleRetriever = new Function<Class<?>, Optional<Bundle>>() {
            @Override public Optional<Bundle> apply(Class<?> input) {
                return Optional.of(bundle);
            }
        };
        OsgiClassPrefixer prefixer = new OsgiClassPrefixer(bundleRetriever);
        assertPresent(prefixer.getPrefix(String.class), "org.apache.brooklyn.my-bundle:");
    }
    
    @Test
    public void testStringPrefix() throws Exception {
        Bundle bundle = Mockito.mock(Bundle.class);
        Mockito.when(bundle.getSymbolicName()).thenReturn("my.symbolic.name");
        Mockito.when(bundle.getVersion()).thenReturn(Version.valueOf("1.2.3"));
        
        OsgiClassPrefixer prefixer = new OsgiClassPrefixer();
        assertAbsent(prefixer.stripMatchingPrefix(bundle, "my.package.MyClass"));
        assertAbsent(prefixer.stripMatchingPrefix(bundle, "different.symbolic.name:my.package.MyClass"));
        assertAbsent(prefixer.stripMatchingPrefix(bundle, "different.symbolic.name:1.2.3:my.package.MyClass"));
        assertPresent(prefixer.stripMatchingPrefix(bundle, "my.symbolic.name:my.package.MyClass"), "my.package.MyClass");
        assertPresent(prefixer.stripMatchingPrefix(bundle, "my.symbolic.name:1.2.3:my.package.MyClass"), "my.package.MyClass");
        
        // TODO Will match any version - is that good enough?
        // Is it the right thing to do, to make upgrades simpler?!
        assertPresent(prefixer.stripMatchingPrefix(bundle, "my.symbolic.name:1.0.0:my.package.MyClass"), "my.package.MyClass");
    }

    @Test
    public void testHasPrefix() throws Exception {
        OsgiClassPrefixer prefixer = new OsgiClassPrefixer();
        assertFalse(prefixer.hasPrefix("MyClassInDefaultPackage"));
        assertFalse(prefixer.hasPrefix("my.package.MyClass"));
        assertTrue(prefixer.hasPrefix("my.symbolic.name:my.package.MyClass"));
        assertTrue(prefixer.hasPrefix("my.symbolic.name:1.2.3:my.package.MyClass"));
    }

    private void assertAbsent(Optional<?> val) {
        assertFalse(val.isPresent(), "val="+val);
    }
    
    private <T> void assertPresent(Optional<T> val, T expected) {
        assertTrue(val.isPresent(), "val="+val);
        assertEquals(val.get(), expected);
    }
}
