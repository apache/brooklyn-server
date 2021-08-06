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

import com.google.common.annotations.Beta;
import java.util.Set;

import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.camp.BasicCampPlatform;
import org.apache.brooklyn.camp.CampPlatform;
import org.apache.brooklyn.camp.brooklyn.api.AssemblyTemplateSpecInstantiator;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.DslParser;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.camp.spi.instantiate.AssemblyTemplateInstantiator;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.typereg.UnsupportedTypePlanException;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.ImmutableSet;

class CampResolver {

    private ManagementContext mgmt;
    private RegisteredType type;
    private RegisteredTypeLoadingContext context;

    // TODO we have a few different modes, detailed below; this logic should be moved to the new transformer
    // and allow specifying which modes are permitted to be in effect?
//    /** whether to allow parsing of the 'full' syntax for applications,
//     * where items are wrapped in a "services:" block, and if the wrapper is an application,
//     * to promote it */
//    boolean allowApplicationFullSyntax = true;
//
//    /** whether to allow parsing of the legacy 'full' syntax, 
//     * where a non-application items are wrapped:
//     * <li> in a "services:" block for entities,
//     * <li> in a "brooklyn.locations" or "brooklyn.policies" block for locations and policies */
//    boolean allowLegacyFullSyntax = true;
//
//    /** whether to allow parsing of the type syntax, where an item is a map with a "type:" field,
//     * i.e. not wrapped in any "services:" or "brooklyn.{locations,policies}" block */
//    boolean allowTypeSyntax = true;

    public CampResolver(ManagementContext mgmt, RegisteredType type, RegisteredTypeLoadingContext context) {
        this.mgmt = mgmt;
        this.type = type;
        this.context = context;
    }

    public AbstractBrooklynObjectSpec<?, ?> createSpec() {
        // TODO new-style approach:
        //            AbstractBrooklynObjectSpec<?, ?> spec = RegisteredTypes.newSpecInstance(mgmt, /* 'type' key */);
        //            spec.configure(keysAndValues);
        return createSpecFromFull(mgmt, type, context.getExpectedJavaSuperType(), context.getAlreadyEncounteredTypes(), context.getLoader());
    }

    @Beta
    public static final ThreadLocal<Stack<RegisteredType>> currentlyCreatingSpec = new ThreadLocal<Stack<RegisteredType>>();

    static AbstractBrooklynObjectSpec<?, ?> createSpecFromFull(ManagementContext mgmt, RegisteredType item, Class<?> expectedType, Set<String> parentEncounteredTypes, BrooklynClassLoadingContext loaderO) {
        // for this method, a prefix "services" or "brooklyn.{location,policies}" is required at the root;
        // we now prefer items to come in "{ type: .. }" format, except for application roots which
        // should have a "services: [ ... ]" block (and which may subsequently be unwrapped)
        BrooklynClassLoadingContext loader = CatalogUtils.newClassLoadingContext(mgmt, item, loaderO);

        Set<String> encounteredTypes;
        // symbolicName could be null if coming from the catalog parser where it tries to load before knowing the id
        if (item.getSymbolicName() != null) {
            encounteredTypes = ImmutableSet.<String>builder()
                .addAll(parentEncounteredTypes)
                .add(item.getSymbolicName())
                .build();
        } else {
            encounteredTypes = parentEncounteredTypes;
        }

        // store the currently creating spec so that we can set the search path on items created by this
        // (messy using a thread local; ideally we'll add it to the API, and/or use the LoadingContext for both this and for encountered types)
        if (currentlyCreatingSpec.get()==null) {
            currentlyCreatingSpec.set(new Stack<>());
        }
        currentlyCreatingSpec.get().push(item);
        try {

            AbstractBrooklynObjectSpec<?, ?> spec;
            String planYaml = RegisteredTypes.getImplementationDataStringForSpec(item);
            MutableSet<Object> supers = MutableSet.copyOf(item.getSuperTypes());
            supers.addIfNotNull(expectedType);
            if (RegisteredTypes.isAnyTypeSubtypeOf(supers, Policy.class)) {
                spec = CampInternalUtils.createPolicySpec(planYaml, loader, encounteredTypes);
            } else if (RegisteredTypes.isAnyTypeSubtypeOf(supers, Enricher.class)) {
                spec = CampInternalUtils.createEnricherSpec(planYaml, loader, encounteredTypes);
            } else if (RegisteredTypes.isAnyTypeSubtypeOf(supers, Location.class)) {
                spec = CampInternalUtils.createLocationSpec(planYaml, loader, encounteredTypes);
            } else if (RegisteredTypes.isAnyTypeSubtypeOf(supers, Application.class)) {
                spec = createEntitySpecFromServicesBlock(mgmt, planYaml, loader, encounteredTypes, true);
            } else if (RegisteredTypes.isAnyTypeSubtypeOf(supers, Entity.class)) {
                spec = createEntitySpecFromServicesBlock(mgmt, planYaml, loader, encounteredTypes, false);
            } else {
                String msg = (item.getSuperTypes()==null || item.getSuperTypes().isEmpty()) ? "no supertypes declared" : "incompatible supertypes "+item.getSuperTypes();
                String itemName = Strings.firstNonBlank(item.getSymbolicName(), BasicBrooklynCatalog.currentlyResolvingType.get(), "<unidentified>");
                throw new IllegalStateException("Cannot create "+itemName+" as spec because "+msg+" ("+currentlyCreatingSpec.get()+")");
            }
            if (expectedType != null && !expectedType.isAssignableFrom(spec.getType())) {
                throw new IllegalStateException("Creating spec from " + item + ", got " + spec.getType() + " which is incompatible with expected " + expectedType);
            }

            spec.stackCatalogItemId(item.getId());

            if (spec instanceof EntitySpec) {
                String name = spec.getDisplayName();
                Object defaultNameConf = spec.getConfig().get(AbstractEntity.DEFAULT_DISPLAY_NAME);
                Object defaultNameFlag = spec.getFlags().get(AbstractEntity.DEFAULT_DISPLAY_NAME.getName());
                if (Strings.isBlank(name) && defaultNameConf == null && defaultNameFlag == null) {
                    spec.configure(AbstractEntity.DEFAULT_DISPLAY_NAME, item.getDisplayName());
                }
            } else {
                // See https://issues.apache.org/jira/browse/BROOKLYN-248, and the tests in
                // ApplicationYamlTest and CatalogYamlLocationTest.
                if (Strings.isNonBlank(item.getDisplayName())) {
                    spec.displayName(item.getDisplayName());
                }
            }

            return spec;

        } finally {
            currentlyCreatingSpec.get().pop();
            if (currentlyCreatingSpec.get().isEmpty()) {
                currentlyCreatingSpec.remove();
            }
        }
    }
 
    private static EntitySpec<?> createEntitySpecFromServicesBlock(ManagementContext mgmt, String plan, BrooklynClassLoadingContext loader, Set<String> encounteredTypes, boolean isApplication) {
        CampPlatform camp = CampInternalUtils.getCampPlatform(loader.getManagementContext());

        // TODO instead of BasicBrooklynCatalog.attemptLegacySpecTransformers where candidate yaml has 'services:' prepended, try that in this method
        // if 'services:' is not declared, but a 'type:' is, then see whether we can parse it with services.

        AssemblyTemplate at = CampInternalUtils.resolveDeploymentPlan(plan, loader, camp);
        AssemblyTemplateInstantiator instantiator = CampInternalUtils.getInstantiator(at);
        if (instantiator instanceof AssemblyTemplateSpecInstantiator) {
            EntitySpec<? extends Application> appSpec = ((AssemblyTemplateSpecInstantiator)instantiator).createApplicationSpec(at, camp, loader, encounteredTypes);

            // above will unwrap but only if it's an Application (and it's permitted);
            // but it doesn't know whether we need an App or if an Entity is okay  
            EntitySpec<? extends Entity> result = !isApplication ? EntityManagementUtils.unwrapEntity(appSpec) : appSpec;
            // if we need an App then definitely *don't* unwrap here because
            // the instantiator will have done that, and it knows if the plan
            // specified a wrapped app explicitly (whereas we don't easily know that here!)

            // it's maybe enough just doing this in BCTR; but feels safer to do it here also, no harm in doing it twice
            fixScopeRootAtRoot(mgmt, result);

            return result;

        } else {
            if (at.getPlatformComponentTemplates()==null || at.getPlatformComponentTemplates().isEmpty()) {
                throw new UnsupportedTypePlanException("No 'services' declared");
            }
            throw new IllegalStateException("Unable to instantiate YAML; invalid type or parameters in plan:\n"+plan);
        }

    }


    static void fixScopeRootAtRoot(ManagementContext mgmt, EntitySpec<?> node) {
        node.getConfig().entrySet().forEach(entry -> {
            fixScopeRoot(mgmt, entry.getValue(), newValue -> node.configure( (ConfigKey) entry.getKey(), newValue));
        });
        node.getFlags().entrySet().forEach(entry -> {
            fixScopeRoot(mgmt, entry.getValue(), newValue -> node.configure( entry.getKey(), newValue));
        });
    }

    private static void fixScopeRoot(ManagementContext mgmt, Object value, Consumer<Object> updater) {
        java.util.function.Function<String,String> fixString = v -> "$brooklyn:self()" + Strings.removeFromStart((String)v, "$brooklyn:scopeRoot()");
        // TODO better approach to replacing scopeRoot
        // we could replace within maps and strings, and inside DSL; currently only supported at root of config or flags
        // but that's hard, we'd need to rebuild those maps and strings, which might be inside objects;
        // and we'd need to replace references to scopeRoot inside a DSL, eg formatString;
        // better would be to collect the DSL items we just created, and convert those if they belong to the now-root node (possibly promoted);
        // it is a rare edge case however, so for now we use this poor-man's logic which captures the most common case --
        // see DslYamlTest.testDslScopeRootEdgeCases
        if (value instanceof BrooklynDslDeferredSupplier) {
            if (value.toString().startsWith("$brooklyn:scopeRoot()")) {
                updater.accept(DslUtils.parseBrooklynDsl(mgmt, fixString.apply(value.toString())));
                return;
            }
        }

        // don't think blocks below here ever get used...
        if (value instanceof String) {
            if (((String)value).startsWith("$brooklyn:scopeRoot()")) {
                updater.accept( fixString.apply((String)value) );
            }
            return;
        }

        if (value instanceof DslComponent) {
            // superseded by above - no longer used
            if ( ((DslComponent)value).getScope() == Scope.SCOPE_ROOT ) {
                updater.accept( DslComponent.newInstanceChangingScope(Scope.THIS, (DslComponent) value, fixString) );
            }
            return;
        }
    }
}