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
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
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


//    public static ConfigKey<Set<EntityInitializer>> INITIALIZERS = new SetConfigKey.Builder(EntityInitializer.class, "member.initializers").build();

    ConfigKey<List<Map<String, Object>>> ENRICHERS = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>(){})
            .name("member.enrichers")
            .defaultValue(ImmutableList.of())
            .build();

    @Override
    protected void onEntityAdded(Entity member) {
        super.onEntityAdded(member);
        ManagementContext mgmt = getManagementContext();

        Maybe<Object> raw = config().getRaw(POLICIES);
        List<Map<String, Object>> maps = raw.isPresent() ? (List<Map<String, Object>>) raw.get() : ImmutableList.of();
        maps.forEach(
                stringObjectMap -> {
                    String type = (String) stringObjectMap.get("type");

                    Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.POLICY.getInterfaceType()));
                    PolicySpec spec;
                    Map<String, Object> brooklynConfig = (Map<String, Object>) stringObjectMap.get("brooklyn.config");

                    if (!item.isNull()) {
                        // throw error if absent for any reason other than null
                        spec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<PolicySpec<Policy>>) BrooklynObjectType.POLICY.getSpecType());
                    } else {
                        spec = PolicySpec.create(ImmutableMap.of(), (Class) new OsgiBrooklynClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    spec.configure(brooklynConfig);


                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(spec);
                    member.policies().add(spec);
                }
        );

//        config().get(INITIALIZERS).forEach(entityInitializer -> {
//            entityInitializer.apply((EntityInternal)member);
//        });


        config().get(INITIALIZERS).forEach(
                stringObjectMap -> {
                    try {
                        Maybe<EntityInitializer> entityInitializerMaybe = BeanWithTypeUtils.tryConvertOrAbsentUsingContext(Maybe.of(stringObjectMap), TypeToken.of(EntityInitializer.class));
                        EntityInitializer initializer = entityInitializerMaybe.get();
                        initializer.apply((EntityInternal) member);
                    }catch(Throwable e){
                        System.out.println(e);
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
                    Map<String, Object> brooklynConfig = (Map<String, Object>) stringObjectMap.get("brooklyn.config");

                    if (!item.isNull()) {
                        // throw error if absent for any reason other than null
                        enricherSpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<EnricherSpec<Enricher>>) BrooklynObjectType.ENRICHER.getSpecType());
                    } else {
                        enricherSpec = EnricherSpec.create(ImmutableMap.of(), (Class) new OsgiBrooklynClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    enricherSpec.configure(brooklynConfig);


                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(enricherSpec);
                    member.enrichers().add(enricherSpec);
                }
        );
    }
}
