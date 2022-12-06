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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.Identifiable;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.ConfigYamlTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.SpecSummary;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.entity.stock.BasicEntityImpl;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.*;


public class NestedRefsCatalogYamlTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(NestedRefsCatalogYamlTest.class);

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testManyAddCatalogItems() throws Exception {

        /* test that invoke-effector doesn't cause confusion */

//        WorkflowBasicTest.addWorkflowStepTypes(mgmt());

        Collection<RegisteredType> typesW = addCatalogItems(ResourceUtils.create(this).getResourceAsString("classpath://nested-refs-catalog-yaml-workflow-test.bom.yaml"));
        Map<RegisteredType, Collection<Throwable>> resultW = mgmt().getCatalog().validateTypes(typesW);
        if (!resultW.isEmpty()) {
            Asserts.fail("Should be empty: " + resultW);
        }

        Collection<RegisteredType> types = addCatalogItems(ResourceUtils.create(this).getResourceAsString("classpath://nested-refs-catalog-yaml-test.bom.yaml"));
        Map<RegisteredType, Collection<Throwable>> result = mgmt().getCatalog().validateTypes(types);
        if (!result.isEmpty()) {
            Asserts.fail("Should be empty: " + result);
        }
    }

}
