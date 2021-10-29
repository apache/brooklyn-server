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
package org.apache.brooklyn.entity.group;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import java.util.List;
import java.util.Map;

public class GroupsChangePolicy extends AbstractMembershipTrackingPolicy {

    public static final ConfigKey<List<Map<String, Object>>> POLICIES = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>(){})
            .name("member.policies")
            .defaultValue(ImmutableList.of())
            .build();

    public static final ConfigKey<List<Map<String, Object>>> INITIALIZERS = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>(){})
            .name("member.initializers")
            .defaultValue(ImmutableList.of())
            .build();

    public static final ConfigKey<List<Map<String, Object>>> ENRICHERS = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>(){})
            .name("member.enrichers")
            .defaultValue(ImmutableList.of())
            .build();

    @Override
    protected void onEntityAdded(Entity member) {
        super.onEntityAdded(member);
        ManagementContext mgmt = getManagementContext();

        Maybe<Object> rawPolicies = config().getRaw(POLICIES);
        List<Map<String, Object>> maps = rawPolicies.isPresent() ? (List<Map<String, Object>>) rawPolicies.get() : ImmutableList.of();
        maps.forEach(
                stringObjectMap -> {
                    String type = (String) stringObjectMap.get("type");

                    Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.POLICY.getInterfaceType()));
                    PolicySpec policySpec;

                    if (!item.isNull()) {
                        policySpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<PolicySpec<Policy>>) BrooklynObjectType.POLICY.getSpecType());
                    } else {
                        policySpec = PolicySpec.create(ImmutableMap.of(), (Class) new OsgiBrooklynClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    policySpec.configure((Map<String, Object>) stringObjectMap.get("brooklyn.config"));


                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(policySpec);
                    member.policies().add(policySpec);
                }
        );

        config().get(INITIALIZERS).forEach(
                stringObjectMap -> {
                    try {
                        OsgiBrooklynClassLoadingContext loader = entity != null ? new OsgiBrooklynClassLoadingContext(entity) : null;
                        EntityInitializer initializer = BeanWithTypeUtils.tryConvertOrAbsent(mgmt,Maybe.of(stringObjectMap), TypeToken.of(EntityInitializer.class), true, loader, true).get();
                        initializer.apply((EntityInternal) member);
                    }catch(Throwable e){
                        e.printStackTrace();
                        throw Exceptions.propagate(e);
                    }
                }
        );

        Maybe<Object> rawEnrichers = config().getRaw(ENRICHERS);
        List<Map<String, Object>> enricherMaps = rawEnrichers.isPresent() ? (List<Map<String, Object>>) rawEnrichers.get() : ImmutableList.of();
        enricherMaps.forEach(
                stringObjectMap -> {
                    String type = (String) stringObjectMap.get("type");
                    Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.ENRICHER.getInterfaceType()));
                    EnricherSpec enricherSpec;

                    if (!item.isNull()) {
                        enricherSpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<EnricherSpec<Enricher>>) BrooklynObjectType.ENRICHER.getSpecType());
                    } else {
                        enricherSpec = EnricherSpec.create(ImmutableMap.of(), (Class) new OsgiBrooklynClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    enricherSpec.configure((Map<String, Object>) stringObjectMap.get("brooklyn.config"));

                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(enricherSpec);
                    member.enrichers().add(enricherSpec);
                }
        );
    }
}
