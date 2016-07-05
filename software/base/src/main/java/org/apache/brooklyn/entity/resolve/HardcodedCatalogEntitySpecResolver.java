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
package org.apache.brooklyn.entity.resolve;

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.resolve.entity.AbstractEntitySpecResolver;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.group.DynamicRegionsFabric;
import org.apache.brooklyn.entity.java.VanillaJavaApp;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.util.core.ClassLoaderUtils;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.collect.ImmutableMap;

public class HardcodedCatalogEntitySpecResolver extends AbstractEntitySpecResolver {
    private static final String RESOLVER_NAME = "catalog";

    private static final Map<String, Class<? extends Entity>> CATALOG_CLASS_TYPES = ImmutableMap.<String, Class<? extends Entity>>builder()
            .put("cluster", DynamicCluster.class)
            .put("fabric", DynamicRegionsFabric.class)
            .put("vanilla", VanillaSoftwareProcess.class)
            .put("software-process", VanillaSoftwareProcess.class)
            .put("java-app", VanillaJavaApp.class)
            .put("brooklyn-node", BrooklynNode.class)
            .build();

    private static final Map<String, String> CATALOG_STRING_TYPES = ImmutableMap.<String, String>builder()
            .put("web-app-cluster","org.apache.brooklyn.software-webapp:" + BrooklynVersion.get() + ":org.apache.brooklyn.entity.webapp.ControlledDynamicWebAppCluster")
            .build();

    // Allow catalog-type or CatalogType as service type string
    private static final Converter<String, String> FMT = CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN);

    public HardcodedCatalogEntitySpecResolver() {
        super(RESOLVER_NAME);
    }

    @Override
    protected boolean canResolve(String type, BrooklynClassLoadingContext loader) {
        String localType = getLocalType(type);
        return getImplementation(CATALOG_CLASS_TYPES, localType) != null ||
                getImplementation(CATALOG_STRING_TYPES, localType) != null;
    }

    @Override
    public EntitySpec<?> resolve(String type, BrooklynClassLoadingContext loader, Set<String> encounteredTypes) {
        String localType = getLocalType(type);
        Class<? extends Entity> specClassType = getImplementation(CATALOG_CLASS_TYPES, localType);
        if (specClassType != null) {
            return buildSpec(specClassType);
        }
        String specStringType = getImplementation(CATALOG_STRING_TYPES, localType);
        if (specStringType != null) {
            return buildSpec(specStringType);
        }
        return null;
    }

    private <T> T getImplementation(Map<String, T> types, String type) {
        T specType = types.get(type);
        if (specType != null) {
            return specType;
        } else {
            return types.get(FMT.convert(type));
        }
    }

    private EntitySpec<?> buildSpec(String specTypName) {
        // TODO is this hardcoded list deprecated? If so log a warning.
        try {
            Class<?> specType = new ClassLoaderUtils(this.getClass()).loadClass(specTypName);
            return buildSpec(specType);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to load hardcoded catalog type " + specTypName, e);
        }
    }

    protected EntitySpec<?> buildSpec(Class<?> specType) {
        @SuppressWarnings("unchecked")
        Class<Entity> specClass = (Class<Entity>)specType;
        return EntitySpec.create(specClass);
    }

}
