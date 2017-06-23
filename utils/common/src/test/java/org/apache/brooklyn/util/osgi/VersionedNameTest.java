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

import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.osgi.framework.Version;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionedNameTest {
    
    @Test
    public void testVersionedNameFromString() {
        VersionedName foo1 = new VersionedName("foo", new Version("1.0"));
        Assert.assertEquals(foo1, VersionedName.fromString("foo:1.0"));
        Assert.assertEquals(foo1, TypeCoercerExtensible.newDefault().coerce("foo:1.0", VersionedName.class));
    }
    
    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testDoesNotAcceptInvalidVersions() {
        Assert.assertEquals(new VersionedName("foo", new Version("1.0.0.alpha")), VersionedName.fromString("foo:1.0-alpha"));
    }
    
    @Test
    public void testManuallyCorrectingVersion() {
        Assert.assertEquals(new VersionedName("foo", new Version("1.0.0.alpha")), VersionedName.fromString("foo:"+
            OsgiUtils.toOsgiVersion("1.0-alpha")));
    }

}
