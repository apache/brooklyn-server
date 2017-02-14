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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.annotations.Test;

public class DeserializingClassRenamesProviderTest {
    
    @Test
    public void testRenameNoopIfNotInProperties() throws Exception {
        assertRename("myname", "myname");
    }
    
    @Test
    public void testRename() throws Exception {
        assertRename("brooklyn.entity.Entity", Entity.class.getName());
    }
    
    @Test
    public void testRenameInnerClass() throws Exception {
        assertRename("brooklyn.util.text.StringPredicates$IsNonBlank", StringPredicates.class.getName() + "$IsNonBlank");
    }
    
    private void assertRename(String orig, String expected) {
        String actual = DeserializingClassRenamesProvider.INSTANCE.findMappedName(orig);
        assertEquals(actual, expected, "orig="+orig);
    }
}
