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

import org.testng.annotations.Test;

public class OsgiUtilsTest {

    // TODO other utils tests ... or maybe other tests in this package are sufficient?
    
    @Test
    public void testToOsgiVersion() {
        assertVersion("0.10.0-20160713.1653", "0.10.0.20160713_1653");

        assertVersion("2.1.0-SNAPSHOT", "2.1.0.SNAPSHOT");
        assertVersion("2.1-SNAPSHOT", "2.1.0.SNAPSHOT");
        assertVersion("0.1-SNAPSHOT", "0.1.0.SNAPSHOT");
        assertVersion("2-SNAPSHOT", "2.0.0.SNAPSHOT");
        assertVersion("2", "2.0.0");
        assertVersion("2.1", "2.1.0");
        assertVersion("2.1.3", "2.1.3");
        assertVersion("2.1.3.4", "2.1.3.4");
        assertVersion("4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
        assertVersion("1.1-alpha-2", "1.1.0.alpha-2");
        assertVersion("1.0-alpha-16-20070122.203121-13", "1.0.0.alpha-16-20070122_203121-13");
        assertVersion("1.0-20070119.021432-1", "1.0.0.20070119_021432-1");
        assertVersion("1-20070119.021432-1", "1.0.0.20070119_021432-1");
        assertVersion("1.4.1-20070217.082013-7", "1.4.1.20070217_082013-7");
        assertVersion("0.0.0.4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
        assertVersion("4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
    }

    @Deprecated
    private void assertVersion(String ver, String expected) {
        assertEquals(OsgiUtils.toOsgiVersion(ver), expected);
    }
}
