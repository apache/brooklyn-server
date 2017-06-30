/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.osgi;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.osgi.framework.Version;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionedNameTest {
    
    @Test
    public void testVersionedNameFromString() {
        VersionedName foo1 = new VersionedName("foo", "1.0");
        Assert.assertEquals(foo1, VersionedName.fromString("foo:1.0"));
        Assert.assertEquals(foo1, TypeCoercerExtensible.newDefault().coerce("foo:1.0", VersionedName.class));
    }
    @Test
    public void testVersionedNameFromVersion() {
        VersionedName foo1 = new VersionedName("foo", new Version("1"));
        Assert.assertEquals(foo1, VersionedName.fromString("foo:1.0.0"));
    }
    
    @Test
    public void testAcceptsAndConvertsNonOsgiVersions() {
        Assert.assertEquals(new VersionedName("foo", new Version("1.0.0.alpha")), 
            VersionedName.toOsgiVersionedName(VersionedName.fromString("foo:1.0-alpha")));
    }
    
    @Test
    public void testParse() throws Exception {
        assertEquals(VersionedName.parseMaybe("a.b", false).get(), new VersionedName("a.b", (String)null));
        try {
            assertEquals(VersionedName.parseMaybe("a.b", true).get(), new VersionedName("a.b", (String)null));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "a.b", "version");
        }
        assertEquals(VersionedName.fromString("a.b:0.1.2"), new VersionedName("a.b", "0.1.2"));
        assertEquals(VersionedName.fromString("a.b:0.0.0.SNAPSHOT"), new VersionedName("a.b", "0.0.0.SNAPSHOT"));
        assertEquals(VersionedName.fromString("a.b:0.0.0_SNAPSHOT"), new VersionedName("a.b", "0.0.0_SNAPSHOT"));
        assertFalse(VersionedName.parseMaybe("a.b:0.1.2:3.4.5", false).isPresent());
        assertFalse(VersionedName.parseMaybe("", false).isPresent());
    }
    
}
