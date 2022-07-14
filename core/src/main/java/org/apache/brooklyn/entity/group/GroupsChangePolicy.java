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
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Policy for adding policies, enrichers, and initializers to entities as the join dynamic groups.
 *
 * Example usage:
 *     - type: org.apache.brooklyn.entity.group.DynamicGroup
 *       name: Empty Software Processes
 *       brooklyn.policies:
 *       - type: org.apache.brooklyn.entity.group.GroupsChangePolicy
 *         brooklyn.config:
 *           group: $brooklyn:self()
 *           member.locations:
 *            - type: org.apache.brooklyn.location.ssh.SshMachineLocation
 *              brooklyn.config:
 *                user: $brooklyn:config("os-user")
 *                address: $brooklyn:attributeWhenReady("host.address")
 *                privateKeyData: $brooklyn:config("ssh-private-key")
 *           member.policies:
 *           - type: org.apache.brooklyn.policy.InvokeEffectorOnSensorChange
 *             brooklyn.config:
 *               sensor.producer: $brooklyn:self()
 *               sensor: host.isCrashed
 *               effector: stop
 *           member.initializers:
 *           - type: org.apache.brooklyn.core.sensor.StaticSensor
 *             brooklyn.config:
 *               name: testsensor
 *               target.type: string
 *               static.value: $brooklyn:formatString("%s%s","test","sensor")
 *           member.enrichers:
 *           - type: org.apache.brooklyn.policy.enricher.HttpLatencyDetector
 *             brooklyn.config:
 *               latencyDetector.url: http://localost:8081
 *               latencyDetector.period: 10s
 *               latencyDetector.requireServiceUp: false
 *
 *       brooklyn.config:
 *         dynamicgroup.entityfilter:
 *           $brooklyn:object:
 *             type: org.apache.brooklyn.core.entity.EntityPredicates
 *             factoryMethod.name: displayNameEqualTo
 *             factoryMethod.args:
 *             - "Empty Software Process"
 */
public class GroupsChangePolicy extends AbstractMembershipTrackingPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(GroupsChangePolicy.class);
    private static final String TYPE = "type";
    private static final String BROOKLYN_CONFIG = "brooklyn.config";

    public static final ConfigKey<List<Map<String, Object>>> LOCATIONS = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>() {})
            .name("member.locations")
            .defaultValue(ImmutableList.of())
            .build();

    public static final ConfigKey<List<Map<String, Object>>> POLICIES = ConfigKeys.builder(new TypeToken<List<Map<String, Object>>>() {})
            .name("member.policies")
            .description("List of policies of the form [{type: policyType, brooklyn.config: {configKey: configValue}}]")
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

        getMaps(LOCATIONS).forEach(
                stringObjectMap -> {
                    try {
                        String type = (String) stringObjectMap.get(TYPE);

                        Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.LOCATION.getInterfaceType()));
                        LocationSpec locationSpec;

                        if (!item.isNull()) {
                            locationSpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<LocationSpec<Location>>) BrooklynObjectType.LOCATION.getSpecType());
                        } else {
                            locationSpec = LocationSpec.create(ImmutableMap.of(), (Class<Location>)
                                    RegisteredTypes.getClassLoadingContext(member).tryLoadClass(type).get());
                        }

                        // NOTE, it is important to resolve all DSL expressions in the context of the member, e.g.
                        // retrieving member specific properties like IP address or credentials.
                        ExecutionContext memberExecutionContext = ((EntityInternal) member).getExecutionContext();
                        Map<String, Object> brooklynConfig = ((Map<String, Object>) stringObjectMap.get(BROOKLYN_CONFIG));
                        ConfigBag configBag = ConfigBag.newInstance(brooklynConfig);
                        brooklynConfig.forEach((key, value) -> {
                            Object resolvedValueFromMember = EntityInitializers.resolve(configBag, ConfigKeys.newConfigKey(Object.class, key), memberExecutionContext);
                            locationSpec.configure(key, resolvedValueFromMember);
                        });

                        AbstractTypePlanTransformer.checkSecuritySensitiveFields(locationSpec);
                        Location location = ((EntityInternal) member).getManagementContext().getLocationManager().createLocation(locationSpec);

                        LOG.info("Applying location '{}' to member '{}'", location, member);
                        ((EntityInternal) member).addLocations(ImmutableList.of(location));
                    } catch (Throwable e) {
                        throw Exceptions.propagate(e);
                    }
                }
        );

        getMaps(INITIALIZERS).forEach(
                stringObjectMap -> {
                    try {
                        String type = (String) stringObjectMap.get(TYPE);

                        BrooklynClassLoadingContext loader = member != null ? RegisteredTypes.getClassLoadingContext(member) : null;
                        TypeToken<? extends EntityInitializer> typeToken = getType(loader, type);
                        LOG.debug("type='{}', typeToken='{}'",type, typeToken);
                        Maybe<? extends EntityInitializer> entityInitializerMaybe = BeanWithTypeUtils.tryConvertOrAbsentUsingContext(Maybe.of(stringObjectMap), typeToken);
                        if (entityInitializerMaybe.isPresent()) {
                            EntityInitializer initializer = entityInitializerMaybe.get();
                            initializer.apply((EntityInternal) member);
                        } else {
                            LOG.debug("Unable to initialize {} due to {}", type, Maybe.getException(entityInitializerMaybe));
                        }
                    } catch (Throwable e) {
                        throw Exceptions.propagate(e);
                    }
                }
        );

        getMaps(POLICIES).forEach(
                stringObjectMap -> {
                    String type = (String) stringObjectMap.get(TYPE);

                    Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.POLICY.getInterfaceType()));
                    PolicySpec policySpec;

                    if (!item.isNull()) {
                        policySpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<PolicySpec<Policy>>) BrooklynObjectType.POLICY.getSpecType());
                    } else {
                        policySpec = PolicySpec.create(ImmutableMap.of(), (Class<Policy>) RegisteredTypes.getClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    policySpec.configure((Map<String, Object>) stringObjectMap.get(BROOKLYN_CONFIG));


                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(policySpec);
                    member.policies().add(policySpec);
                }
        );

        getMaps(ENRICHERS).forEach(
                stringObjectMap -> {
                    String type = (String) stringObjectMap.get(TYPE);
                    Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(BrooklynObjectType.ENRICHER.getInterfaceType()));
                    EnricherSpec enricherSpec;

                    if (!item.isNull()) {
                        enricherSpec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<EnricherSpec<Enricher>>) BrooklynObjectType.ENRICHER.getSpecType());
                    } else {
                        enricherSpec = EnricherSpec.create(ImmutableMap.of(), (Class<Enricher>) RegisteredTypes.getClassLoadingContext(entity).tryLoadClass(type).get());
                    }
                    enricherSpec.configure((Map<String, Object>) stringObjectMap.get(BROOKLYN_CONFIG));

                    AbstractTypePlanTransformer.checkSecuritySensitiveFields(enricherSpec);
                    member.enrichers().add(enricherSpec);
                }
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> TypeToken<T> getType(BrooklynClassLoadingContext loader, String className) {
        return (TypeToken<T>) new BrooklynTypeNameResolution.BrooklynTypeNameResolver("", loader, true, true).getTypeToken(className);
    }

    private List<Map<String, Object>> getMaps(ConfigKey<List<Map<String, Object>>> key) {
        Maybe<Object> rawInitializers = config().getRaw(key);
        return rawInitializers.isPresent() ? (List<Map<String, Object>>) rawInitializers.get() : ImmutableList.of();
    }

    /** default to the entity where attached, eg if attached to a group no need to specify $brooklyn:self() */
    protected Group getGroup() {
        Group result = super.getGroup();
        if (result!=null) return result;
        if (entity instanceof Group) return (Group)entity;
        return null;
    }

}
