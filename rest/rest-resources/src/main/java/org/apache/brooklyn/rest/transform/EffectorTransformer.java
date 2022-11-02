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
package org.apache.brooklyn.rest.transform;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.rest.api.ApplicationApi;
import org.apache.brooklyn.rest.api.EffectorApi;
import org.apache.brooklyn.rest.api.EntityApi;
import org.apache.brooklyn.rest.domain.EffectorSummary;
import org.apache.brooklyn.rest.domain.EffectorSummary.ParameterSummary;
import org.apache.brooklyn.rest.resources.AbstractBrooklynRestResource;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Set;

import static org.apache.brooklyn.rest.util.WebResourceUtils.serviceUriBuilder;

public class EffectorTransformer {

    public static EffectorSummary effectorSummary(final Entity entity, Effector<?> effector, UriBuilder ub) {
        URI applicationUri = serviceUriBuilder(ub, ApplicationApi.class, "get").build(entity.getApplicationId());
        URI entityUri = serviceUriBuilder(ub, EntityApi.class, "get").build(entity.getApplicationId(), entity.getId());
        URI selfUri = serviceUriBuilder(ub, EffectorApi.class, "invoke").build(entity.getApplicationId(), entity.getId(), effector.getName());
        return new EffectorSummary(effector.getName(), effector.getReturnTypeName(),
                 ImmutableSet.copyOf(Iterables.transform(effector.getParameters(),
                new Function<ParameterType<?>, EffectorSummary.ParameterSummary<?>>() {
                    @Override
                    public EffectorSummary.ParameterSummary<?> apply(@Nullable ParameterType<?> parameterType) {
                        return parameterSummary(((EntityInternal)entity).getManagementContext(), entity, parameterType);
                    }
                })), effector.getDescription(), ImmutableMap.of(
                "self", selfUri,
                "entity", entityUri,
                "application", applicationUri
        ));
    }

    public static EffectorSummary effectorSummaryForCatalog(@Nullable /* if called from CLI */ ManagementContext mgmt, Effector<?> effector) {
        Set<EffectorSummary.ParameterSummary<?>> parameters = ImmutableSet.copyOf(Iterables.transform(effector.getParameters(),
                new Function<ParameterType<?>, EffectorSummary.ParameterSummary<?>>() {
                    @Override
                    public EffectorSummary.ParameterSummary<?> apply(ParameterType<?> parameterType) {
                        return parameterSummary(mgmt, null, parameterType);
                    }
                }));
        return new EffectorSummary(effector.getName(),
                effector.getReturnTypeName(), parameters, effector.getDescription(), null);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static EffectorSummary.ParameterSummary<?> parameterSummary(ManagementContext mgmt, @Nullable Entity entity, ParameterType<?> parameterType) {
        try {
            Maybe<?> defaultValue = Tasks.resolving(parameterType.getDefaultValue())
                    .as(parameterType.getParameterType())
                    .context(entity)
                    .immediately(true)
                    .getMaybe();
            boolean isSecret = Sanitizer.IS_SECRET_PREDICATE.apply(parameterType.getName());
            return new ParameterSummary(parameterType.getName(), parameterType.getParameterClassName(), 
                parameterType.getDescription(),
                mgmt==null ? /* should only be null if called from CLI context */ defaultValue.orNull() :
                    AbstractBrooklynRestResource.RestValueResolver.resolving(mgmt, null).getValueForDisplay(defaultValue.orNull(), true, false,
                        isSecret ? false : null),
                isSecret);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
}
