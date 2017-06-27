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
package org.apache.brooklyn.camp.brooklyn.catalog;

import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

/** As parent tests, but using OSGi, and some of the additions are stricter / different */ 
public class CatalogOsgiYamlVersioningTest extends CatalogYamlVersioningTest {
    
    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Override
    @Test
    public void testAddSameVersionWithoutBundle() {
        try {
            // parent test should fail in OSGi
            super.testAddSameVersionWithoutBundle();
            Asserts.shouldHaveFailedPreviously("Expected to fail because containing bundle will be different when using OSGi");
        } catch (Exception e) {
            assertExpectedFailureSaysUpdatingExistingItemForbidden(e);
            assertExpectedFailureIncludesSampleId(e);
        }
    }
    
    @Test
    public void testAddSameVersionWithoutBundleWorksIfForced() {
        String symbolicName = "sampleId";
        String version = "0.1.0";
        addCatalogEntityWithoutBundle(symbolicName, version);
        forceCatalogUpdate();
        addCatalogEntityWithoutBundle(symbolicName, version);
    }
    

    @Override
    protected void checkAddSameVersionFailsWhenIconIsDifferent(Exception e) {
        Asserts.expectedFailureContainsIgnoreCase(e, 
            "cannot install a different bundle at a same non-snapshot version");
        assertExpectedFailureIncludesSampleId(e);
    }
}
