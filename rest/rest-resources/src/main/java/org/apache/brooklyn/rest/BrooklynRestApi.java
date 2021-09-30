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
package org.apache.brooklyn.rest;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

import java.util.Set;
import org.apache.brooklyn.rest.resources.*;
import org.apache.brooklyn.rest.util.DefaultExceptionMapper;
import org.apache.brooklyn.rest.util.FormMapProvider;
import org.apache.brooklyn.rest.util.json.BrooklynJacksonJsonProvider;

import com.google.common.collect.Iterables;

import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.apache.brooklyn.util.collections.MutableSet;

public class BrooklynRestApi {

    public static Iterable<AbstractBrooklynRestResource> getBrooklynRestResources() {
        List<AbstractBrooklynRestResource> resources = new ArrayList<>();
        resources.add(new LocationResource());
        resources.add(new CatalogResource());
        resources.add(new TypeResource());
        resources.add(new BundleResource());
        resources.add(new ApplicationResource());
        resources.add(new EntityResource());
        resources.add(new EntityConfigResource());
        resources.add(new SensorResource());
        resources.add(new EffectorResource());
        resources.add(new AdjunctResource());
        resources.add(new PolicyResource());
        resources.add(new PolicyConfigResource());
        resources.add(new ActivityResource());
        resources.add(new AccessResource());
        resources.add(new ScriptResource());
        resources.add(new ServerResource());
        resources.add(new UsageResource());
        resources.add(new LogoutResource());
        resources.add(new LogbookResource());
        return resources;
    }

    public static Iterable<Object> getApidocResources() {
        List<Object> resources = new ArrayList<Object>();
        resources.add(new SwaggerSerializers());
        resources.add(new ApidocResource());
        return resources;
    }

    public static Iterable<Object> getMiscResources() {
        List<Object> resources = new ArrayList<Object>();
        resources.add(new DefaultExceptionMapper());
        resources.add(new BrooklynJacksonJsonProvider());
        resources.add(new FormMapProvider());
        return resources;
    }

    static Set<Object> extraResources = MutableSet.of();

    @VisibleForTesting
    public static void addExtraResource(Object resource) {
        extraResources.add(resource);
    }
    @VisibleForTesting
    public static Set<Object> getExtraResourcesMutable() {
        return extraResources;
    }

    public static Set<Object> getExtraResources() {
        return MutableSet.copyOf(extraResources);
    }

    public static Iterable<Object> getAllResources() {
        return Iterables.concat(getBrooklynRestResources(), getApidocResources(), getMiscResources(), getExtraResources());
    }
}
