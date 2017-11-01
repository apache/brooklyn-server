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
package org.apache.brooklyn.camp.brooklyn.spi.creation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.camp.CampPlatform;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatform;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampReservedKeys;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.camp.spi.AssemblyTemplate.Builder;
import org.apache.brooklyn.camp.spi.instantiate.AssemblyTemplateInstantiator;
import org.apache.brooklyn.camp.spi.pdp.DeploymentPlan;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog.BrooklynLoaderTracker;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.entity.stock.BasicApplicationImpl;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.yaml.Yamls;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/** package-private; as {@link RegisteredType} becomes standard hopefully we can remove this */
class CampInternalUtils {

    static EntitySpec<? extends Application> createWrapperApp(AssemblyTemplate template, BrooklynClassLoadingContext loader) {
        BrooklynComponentTemplateResolver resolver = BrooklynComponentTemplateResolver.Factory.newInstance(loader, buildWrapperAppTemplate(template));
        EntitySpec<Application> wrapperSpec = resolver.resolveSpec(ImmutableSet.<String>of());
        resetSpecIfTemplateHasNoExplicitParameters(template, wrapperSpec);
        // caller always sets WRAPPER_APP config; should we do it here?
        return wrapperSpec;
    }

    static void resetSpecIfTemplateHasNoExplicitParameters(AssemblyTemplate template, EntitySpec<? extends Application> wrapperSpec) {
        if (!template.getCustomAttributes().containsKey(BrooklynCampReservedKeys.BROOKLYN_PARAMETERS)) {
            // Clear out default parameters (coming from the wrapper app's class) so they don't overwrite the entity's params on unwrap.
            wrapperSpec.parameters(ImmutableList.<SpecParameter<?>>of());
        }
    }

    private static AssemblyTemplate buildWrapperAppTemplate(AssemblyTemplate template) {
        Builder<? extends AssemblyTemplate> builder = AssemblyTemplate.builder();
        builder.type("brooklyn:" + BasicApplicationImpl.class.getName());
        builder.id(template.getId());
        builder.name(template.getName());
        builder.sourceCode(template.getSourceCode());
        for (Entry<String, Object> entry : template.getCustomAttributes().entrySet()) {
            builder.customAttribute(entry.getKey(), entry.getValue());
        }
        builder.instantiator(template.getInstantiator());
        AssemblyTemplate wrapTemplate = builder.build();
        return wrapTemplate;
    }

    static AssemblyTemplateInstantiator getInstantiator(AssemblyTemplate at) {
        try {
            return at.getInstantiator().newInstance();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    static AssemblyTemplate resolveDeploymentPlan(String plan, BrooklynClassLoadingContext loader, CampPlatform camp) {
        BrooklynLoaderTracker.setLoader(loader);
        try {
            return camp.pdp().resolveDeploymentPlan(new StringReader(plan));
        } finally {
            BrooklynLoaderTracker.unsetLoader(loader);
        }
    }

    static PolicySpec<?> createPolicySpec(String yamlPlan, BrooklynClassLoadingContext loader, Set<String> encounteredCatalogTypes) {
        DeploymentPlan plan = makePlanFromYaml(loader.getManagementContext(), yamlPlan);

        //Would ideally re-use the PolicySpecResolver
        //but it is CAMP specific and there is no easy way to get hold of it.
        Object policies = checkNotNull(plan.getCustomAttributes().get(BasicBrooklynCatalog.POLICIES_KEY), "policy config");
        if (!(policies instanceof Iterable<?>)) {
            throw new IllegalStateException("The value of " + BasicBrooklynCatalog.POLICIES_KEY + " must be an Iterable.");
        }

        Object policy = Iterables.getOnlyElement((Iterable<?>)policies);

        return createPolicySpec(loader, policy, encounteredCatalogTypes);
    }

    @SuppressWarnings("unchecked")
    static PolicySpec<?> createPolicySpec(BrooklynClassLoadingContext loader, Object policy, Set<String> encounteredCatalogTypes) {
        Map<String, Object> itemMap;
        if (policy instanceof String) {
            itemMap = ImmutableMap.<String, Object>of("type", policy);
        } else if (policy instanceof Map) {
            itemMap = (Map<String, Object>) policy;
        } else {
            throw new IllegalStateException("Policy expected to be string or map. Unsupported object type " + policy.getClass().getName() + " (" + policy.toString() + ")");
        }

        String versionedId = (String) checkNotNull(Yamls.getMultinameAttribute(itemMap, "policy_type", "policyType", "type"), "policy type");
        PolicySpec<? extends Policy> spec = resolvePolicySpec(versionedId, loader, encounteredCatalogTypes);
        initConfigAndParameters(spec, itemMap, loader);
        return spec;
    }

    static EnricherSpec<?> createEnricherSpec(String yamlPlan, BrooklynClassLoadingContext loader, Set<String> encounteredCatalogTypes) {
        DeploymentPlan plan = makePlanFromYaml(loader.getManagementContext(), yamlPlan);

        //Would ideally re-use the EnricherSpecResolver
        //but it is CAMP specific and there is no easy way to get hold of it.
        Object enrichers = checkNotNull(plan.getCustomAttributes().get(BasicBrooklynCatalog.ENRICHERS_KEY), "enricher config");
        if (!(enrichers instanceof Iterable<?>)) {
            throw new IllegalStateException("The value of " + BasicBrooklynCatalog.ENRICHERS_KEY + " must be an Iterable.");
        }

        Object policy = Iterables.getOnlyElement((Iterable<?>)enrichers);

        return createEnricherSpec(loader, policy, encounteredCatalogTypes);
    }

    @SuppressWarnings("unchecked")
    static EnricherSpec<?> createEnricherSpec(BrooklynClassLoadingContext loader, Object enricher, Set<String> encounteredCatalogTypes) {
        Map<String, Object> itemMap;
        if (enricher instanceof String) {
            itemMap = ImmutableMap.<String, Object>of("type", enricher);
        } else if (enricher instanceof Map) {
            itemMap = (Map<String, Object>) enricher;
        } else {
            throw new IllegalStateException("Enricher expected to be string or map. Unsupported object type " + enricher.getClass().getName() + " (" + enricher.toString() + ")");
        }

        String versionedId = (String) checkNotNull(Yamls.getMultinameAttribute(itemMap, "enricher_type", "enricherType", "type"), "enricher type");
        EnricherSpec<? extends Enricher> spec = resolveEnricherSpec(versionedId, loader, encounteredCatalogTypes);
        initConfigAndParameters(spec, itemMap, loader);
        return spec;
    }

    static LocationSpec<?> createLocationSpec(String yamlPlan, BrooklynClassLoadingContext loader, Set<String> encounteredTypes) {
        DeploymentPlan plan = makePlanFromYaml(loader.getManagementContext(), yamlPlan);
        Object locations = checkNotNull(plan.getCustomAttributes().get(BasicBrooklynCatalog.LOCATIONS_KEY), "location config");
        if (!(locations instanceof Iterable<?>)) {
            throw new IllegalStateException("The value of " + BasicBrooklynCatalog.LOCATIONS_KEY + " must be an Iterable.");
        }

        Object location = Iterables.getOnlyElement((Iterable<?>)locations);
        return createLocationSpec(loader, location);
    }

    @SuppressWarnings("unchecked")
    private static LocationSpec<?> createLocationSpec(BrooklynClassLoadingContext loader, Object location) {
        Map<String, Object> itemMap;
        if (location instanceof String) {
            itemMap = ImmutableMap.<String, Object>of("type", location);
        } else if (location instanceof Map) {
            itemMap = (Map<String, Object>) location;
        } else {
            throw new IllegalStateException("Location expected to be string or map. Unsupported object type " + location.getClass().getName() + " (" + location.toString() + ")");
        }

        String type = (String) checkNotNull(Yamls.getMultinameAttribute(itemMap, "location_type", "locationType", "type"), "location type");
        Map<String, Object> brooklynConfig = (Map<String, Object>) itemMap.get(BrooklynCampReservedKeys.BROOKLYN_CONFIG);
        LocationSpec<?> locationSpec = resolveLocationSpec(type, brooklynConfig, loader);
        // config loaded twice, but used twice
        initConfigAndParameters(locationSpec, itemMap, loader);
        return locationSpec;
    }

    protected static void initConfigAndParameters(AbstractBrooklynObjectSpec<?,?> spec, Map<String, Object> itemMap, BrooklynClassLoadingContext loader) {
        @SuppressWarnings("unchecked")
        Map<String, Object> brooklynConfig = (Map<String, Object>) itemMap.get(BrooklynCampReservedKeys.BROOKLYN_CONFIG);
        if (brooklynConfig != null) {
            spec.configure(brooklynConfig);
        }

        List<?> explicitParams = (List<?>) itemMap.get(BrooklynCampReservedKeys.BROOKLYN_PARAMETERS);
        BasicSpecParameter.initializeSpecWithExplicitParameters(spec, BasicSpecParameter.parseParameterDefinitionList(explicitParams, null, loader), loader);
    }

    public static DeploymentPlan makePlanFromYaml(ManagementContext mgmt, String yaml) {
        CampPlatform camp = getCampPlatform(mgmt);
        return camp.pdp().parseDeploymentPlan(Streams.newReaderWithContents(yaml));
    }

    @Nonnull public static CampPlatform getCampPlatform(ManagementContext mgmt) {
        CampPlatform result = BrooklynCampPlatform.findPlatform(mgmt);
        if (result!=null) {
            return result;
        } else {
            throw new IllegalStateException("No CAMP Platform is registered with this Brooklyn management context.");
        }
    }

    @SuppressWarnings("unchecked")
    private static PolicySpec<? extends Policy> resolvePolicySpec(
            String versionedId,
            BrooklynClassLoadingContext loader,
            Set<String> encounteredCatalogTypes) {
        
        PolicySpec<? extends Policy> spec;
        RegisteredType item = loader.getManagementContext().getTypeRegistry().get(
            CatalogUpgrades.getTypeUpgradedIfNecessary(loader.getManagementContext(), versionedId));
        if (item != null && !encounteredCatalogTypes.contains(item.getSymbolicName())) {
            RegisteredTypeLoadingContext context = RegisteredTypeLoadingContexts.alreadyEncountered(encounteredCatalogTypes);
            return loader.getManagementContext().getTypeRegistry().createSpec(item, context, PolicySpec.class);
        } else {
            // TODO-type-registry pass the loader in to the above, and allow it to load with the loader
            spec = PolicySpec.create(loader.loadClass(versionedId, Policy.class));
        }
        return spec;
    }

    @SuppressWarnings("unchecked")
    private static EnricherSpec<? extends Enricher> resolveEnricherSpec(
            String versionedId,
            BrooklynClassLoadingContext loader,
            Set<String> encounteredCatalogTypes) {

        EnricherSpec<? extends Enricher> spec;
        RegisteredType item = loader.getManagementContext().getTypeRegistry().get(
            CatalogUpgrades.getTypeUpgradedIfNecessary(loader.getManagementContext(), versionedId));
        if (item != null && !encounteredCatalogTypes.contains(item.getSymbolicName())) {
            RegisteredTypeLoadingContext context = RegisteredTypeLoadingContexts.alreadyEncountered(encounteredCatalogTypes);
            return loader.getManagementContext().getTypeRegistry().createSpec(item, context, EnricherSpec.class);
        } else {
            // TODO-type-registry pass the loader in to the above, and allow it to load with the loader
            spec = EnricherSpec.create(loader.loadClass(versionedId, Enricher.class));
        }
        return spec;
    }

    private static LocationSpec<?> resolveLocationSpec(
            String type,
            Map<String, Object> brooklynConfig,
            BrooklynClassLoadingContext loader) {
        if (brooklynConfig==null) {
            brooklynConfig = MutableMap.of();
        }
        Maybe<Class<? extends Location>> javaClass = loader.tryLoadClass(type, Location.class);
        LocationSpec<?> spec;
        if (javaClass.isPresent()) {
            spec = LocationSpec.create(javaClass.get());
        } else {
            Maybe<LocationSpec<? extends Location>> loc = loader.getManagementContext().getLocationRegistry().getLocationSpec(type, brooklynConfig);
            if (loc.isPresent()) {
                spec = loc.get();
            } else {
                throw new IllegalStateException("No class or resolver found for location type "+type);
            }
        }
        return spec;
    }

}
