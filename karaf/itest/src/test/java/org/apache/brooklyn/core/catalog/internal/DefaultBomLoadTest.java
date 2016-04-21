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
package org.apache.brooklyn.core.catalog.internal;


import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.test.IntegrationTest;
import org.apache.karaf.features.BootFinished;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.ops4j.pax.exam.util.Filter;

import javax.inject.Inject;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
@Category(IntegrationTest.class)
public class DefaultBomLoadTest {

    /**
     * To make sure the tests run only when the boot features are fully installed
     */
    @Inject
    @Filter(timeout = 120000)
    BootFinished bootFinished;

    @Inject
    protected ManagementContext managementContext;


    @Configuration
    public static Option[] configuration() throws Exception {
        return defaultOptionsWith(
            // Uncomment this for remote debugging the tests on port 5005
//             , KarafDistributionOption.debugConfiguration()
        );
    }


    @Test
    @Category(IntegrationTest.class)
    public void shouldHaveLoadedDefaultCatalogBom() throws Exception {
        final CatalogItem<?, ?> catalogItem = managementContext.getCatalog()
            .getCatalogItem("server-template", BrooklynVersion.get());  // from brooklyn-software-base catalog.bom
        assertNotNull(catalogItem);
        assertEquals("Template: Server", catalogItem.getDisplayName());
    }
}
