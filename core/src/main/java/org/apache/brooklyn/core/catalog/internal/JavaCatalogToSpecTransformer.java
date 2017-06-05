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

import java.util.Set;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.classloading.BrooklynClassLoadingContextSequential;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.plan.PlanToSpecTransformer;
import org.apache.brooklyn.core.typereg.UnsupportedTypePlanException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instantiates classes from a registered type which simply
 * defines the java class name and OSGi bundles to use.
 * <p>
 * This is used where a {@link RegisteredType} is defined simply with the name of a java class
 * (no YAML etc).
 */
public class JavaCatalogToSpecTransformer implements PlanToSpecTransformer {
    private static final Logger log = LoggerFactory.getLogger(JavaCatalogToSpecTransformer.class);

    private ManagementContext mgmt;

    @Override
    public void setManagementContext(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    @Override
    public String getShortDescription() {
        return "Java type instantiator";
    }

    @Override
    public boolean accepts(String planType) {
        return false;
    }

    @Override
    public EntitySpec<? extends Application> createApplicationSpec(String plan) throws UnsupportedTypePlanException {
        throw new UnsupportedTypePlanException(getClass().getName() + " doesn't parse application plans.");
    }

    @Override
    public <T, SpecT extends AbstractBrooklynObjectSpec<? extends T, SpecT>> SpecT createCatalogSpec(
            CatalogItem<T, SpecT> item, Set<String> encounteredTypes) throws UnsupportedTypePlanException {
        @SuppressWarnings("deprecation")
        String javaType = item.getJavaType();
        if (javaType != null) {
            // TODO This log.warn previously said "Using old-style xml catalog items with java type attribute".
            // However, nothing in this code is specific to catalog.xml. Perhaps that is the only way this code
            // can be reached? We should investigate further when we have time - if we can confirm it was only
            // used for catalog.xml, then this can be deleted.
            log.warn("Deprecated functionality (since 0.9.0). Using old-style java type attribute for " + item);
            Class<?> type;
            try {
                // java types were deprecated before we added osgi support so this isn't necessary,
                // but it doesn't hurt (and if we re-instate a class+bundle approach for RegisteredType 
                // we will want to do this)
                final BrooklynClassLoadingContextSequential ctx = new BrooklynClassLoadingContextSequential(mgmt);
                ctx.add(CatalogUtils.newClassLoadingContextForCatalogItems(mgmt, item.getCatalogItemId(),
                    item.getCatalogItemIdSearchPath()));
                type = ctx.loadClass(javaType);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new IllegalStateException("Unable to load old-style java catalog item type " + javaType + " for item " + item, e);
            }
            AbstractBrooklynObjectSpec<?,?> spec;
            if (Entity.class.isAssignableFrom(type)) {
                @SuppressWarnings("unchecked")
                Class<Entity> entityType = (Class<Entity>)type;
                spec = EntitySpec.create(entityType)
                        .parameters(BasicSpecParameter.fromClass(mgmt, entityType));
            } else if (Policy.class.isAssignableFrom(type)) {
                @SuppressWarnings("unchecked")
                Class<Policy> policyType = (Class<Policy>)type;
                spec = PolicySpec.create(policyType);
            } else if (Enricher.class.isAssignableFrom(type)) {
                @SuppressWarnings("unchecked")
                Class<Enricher> enricherType = (Class<Enricher>)type;
                spec = EnricherSpec.create(enricherType);
            } else {
                throw new IllegalStateException("Catalog item " + item + " java type " + javaType + " is not a Brooklyn supported object.");
            }
            spec.catalogItemIdAndSearchPath(item.getCatalogItemId(), item.getCatalogItemIdSearchPath());
            @SuppressWarnings("unchecked")
            SpecT untypedSpc = (SpecT) spec;
            return untypedSpc;
        } else {
            throw new UnsupportedTypePlanException(getClass().getName() + " parses only old-style catalog items containing javaType");
        }
    }

}
