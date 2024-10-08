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
package org.apache.brooklyn.core.objs.proxy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.brooklyn.api.entity.*;
import org.apache.brooklyn.api.internal.BrooklynLoggingCategories;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.EntityManager;
import org.apache.brooklyn.api.mgmt.EntityManager.EntityCreationOptions;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConstraintViolationException;
import org.apache.brooklyn.core.entity.*;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.javalang.AggregateClassLoader;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Creates entities (and proxies) of required types, given the
 *
 * This is an internal class for use by core-brooklyn. End-users are strongly discouraged from
 * using this class directly.
 *
 * Used in three situations:
 * <ul>
 *   <li>Normal entity creation (through entityManager.createEntity)
 *   <li>rebind (i.e. Brooklyn restart, or promotion of HA standby manager node)
 *   <li>yaml parsing
 * </ul>
 *
 * @author aled
 */
public class InternalEntityFactory extends InternalFactory {

    private static final Logger log = LoggerFactory.getLogger(InternalEntityFactory.class);

    private final EntityTypeRegistry entityTypeRegistry;
    private final InternalPolicyFactory policyFactory;
    private final ClassLoaderCache classLoaderCache;

    /**
     * The initializers to be added to any application deployed by Brooklyn.
     * e.g. <code>brooklyn.deployment.initializers=org.apache.brooklyn.core.effector.AddDeploySensorsInitializer</code>
     * will automatically add a sensor to the root node of any deployed application. It's value is a JSON payload containing
     * information about who and when the application has been deployed.
     */
    public final static ConfigKey<String> GLOBAL_DEPLOYMENT_INITIALIZER_CLASSNAMES = ConfigKeys.newStringConfigKey(
            "brooklyn.deployment.initializers",
            "Comma separated list of class names corresponding to Brooklyn Initializers to be automatically added and ran on every application deployed",
            "");

    public InternalEntityFactory(ManagementContextInternal managementContext, EntityTypeRegistry entityTypeRegistry, InternalPolicyFactory policyFactory) {
        super(managementContext);
        this.entityTypeRegistry = checkNotNull(entityTypeRegistry, "entityTypeRegistry");
        this.policyFactory = checkNotNull(policyFactory, "policyFactory");
        this.classLoaderCache = new ClassLoaderCache();
    }

    @VisibleForTesting
    public <T extends Entity> T createEntityProxy(EntitySpec<T> spec, T entity) {
        Set<Class<?>> interfaces = Sets.newLinkedHashSet();
        if (spec.getType().isInterface()) {
            interfaces.add(spec.getType());
        } else {
            log.warn("EntitySpec declared in terms of concrete type "+spec.getType()+"; should be supplied in terms of interface");
            interfaces.addAll(Reflections.getAllInterfaces(spec.getType()));
        }
        interfaces.addAll(spec.getAdditionalInterfaces());

        return createEntityProxy(interfaces, entity);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Entity> T createEntityProxy(Iterable<Class<?>> interfaces, T entity) {
        // We don't especially want the proxy to have to implement EntityLocal, 
        // but required by how AbstractEntity.parent is used (e.g. parent.getAllConfig).
        // However within EntityProxyImpl we place add'l guards to prevent read-only access to such methods
        Set<Class<?>> allInterfaces = MutableSet.<Class<?>>builder()
                .add(EntityProxy.class, Entity.class, EntityLocal.class, EntityInternal.class)
                .addAll(interfaces)
                .build();

        // TODO OSGi strangeness! The classloader obtained from the type should be enough.
        // If an OSGi class loader, it should delegate to find things like Entity.class etc.
        // However, we get errors such as:
        //    NoClassDefFoundError: org.apache.brooklyn.api.sensor.AttributeSensor not found by ....brooklyn-test-osgi-entities
        // Building our own aggregating class loader gets around this.
        // But we really should not have to do this! What are the consequences?
        //
        // The reason for the error is that the proxy tries to load all classes
        // referenced from the entity and its interfaces with the single passed loader
        // while a normal class loading would nest the class loaders (loading interfaces'
        // references with their own class loaders which in our case are different).
        AggregateClassLoader aggregateClassLoader = classLoaderCache.getClassLoaderForProxy(entity.getClass(), allInterfaces);

        return (T) java.lang.reflect.Proxy.newProxyInstance(
                aggregateClassLoader,
                allInterfaces.toArray(new Class[allInterfaces.size()]),
                new EntityProxyImpl(entity));
    }

    public <T extends Entity> T createEntity(EntitySpec<T> spec) {
        return createEntity(spec, new EntityManager.EntityCreationOptions() {});
    }

    public <T extends Entity> T createEntity(EntitySpec<T> spec, String entityId) {
        return createEntity(spec, new EntityManager.EntityCreationOptions() {
            @Override
            public String getRequiredUniqueId() {
                return entityId;
            }
        });
    }

    /** creates a new entity instance from a spec, with all children, policies, etc,
     * fully initialized ({@link AbstractEntity#init()} invoked) and ready for
     * management -- commonly the caller will next call
     * {@link Entities#manage(Entity)} (if it's in a managed application)
     * or {@link Entities#startManagement(org.apache.brooklyn.api.entity.Application, org.apache.brooklyn.api.mgmt.ManagementContext)}
     * (if it's an application) */
    public <T extends Entity> T createEntity(EntitySpec<T> spec, EntityManager.EntityCreationOptions options) {
        /* Order is important here. Changed Jul 2014 when supporting children in spec.
         * (Previously was much simpler, and parent was set right after running initializers; and there were no children.)
         * <p>
         * It seems we need access to the parent (indeed the root application) when running some initializers (esp children initializers).
         * <p>
         * Now we do two passes, so that hierarchy is fully populated before initialization and policies.
         * (This is needed where some config or initializer might reference another entity by its ID, e.g. yaml $brooklyn:component("id").
         * Initialization is done in parent-first order with depth-first children traversal.
         */

        if (options==null) options = new EntityManager.EntityCreationOptions() {};

        // (maps needed because we need the spec, and we need to keep the AbstractEntity to call init, not a proxy)
        Map<String,Entity> entitiesByEntityId = MutableMap.of();
        Map<String,EntitySpec<?>> specsByEntityId = MutableMap.of();


        T entity = createEntityAndDescendantsUninitialized(0, spec, options, entitiesByEntityId, specsByEntityId);
        try {
            // prevent this from blocking
            Thread.currentThread().interrupt();
            try {
                initEntityAndDescendants(entity.getId(), entitiesByEntityId, specsByEntityId, options, true);
            } finally {
                // end of non-blocking portion
                Thread.interrupted();
            }
        } catch (RuntimeException ex) {
            // end of non-blocking portion
            Thread.interrupted();

            options.onException(ex, (e) -> {
                Exceptions.propagateIfFatal(e);
                log.info("Failed to initialise entity " + entity + " and its descendants - unmanaging and propagating original exception: " + Exceptions.collapseText(e));
                try {
                    if (managementContext.isRunning())
                        ((EntityManagerInternal) managementContext.getEntityManager()).discardPremanaged(entity);
                } catch (Exception e2) {
                    Exceptions.propagateIfFatal(e2);
                    log.info("Failed to unmanage entity " + entity + " and its descendants, after failure to initialise (rethrowing original exception)", e2);
                }
                throw e;
            });
        }

        return entity;
    }

    private <T extends Entity> T createEntityAndDescendantsUninitialized(int depth, EntitySpec<T> spec, EntityManager.EntityCreationOptions options,
            Map<String,Entity> entitiesByEntityId, Map<String,EntitySpec<?>> specsByEntityId) {

        T entity = null;

        try {
            if (spec.getFlags().containsKey("parent") || spec.getFlags().containsKey("owner")) {
                throw new IllegalArgumentException("Spec's flags must not contain parent or owner; use spec.parent() instead for "+spec);
            }
            if (spec.getFlags().containsKey("id")) {
                throw new IllegalArgumentException("Spec's flags must not contain id; use spec.id() instead for "+spec);
            }

            Class<? extends T> clazz = getImplementedBy(spec);

            entity = constructEntity(clazz, spec, depth==0 ? options.getRequiredUniqueId() : null);

            if (!options.isDryRun()) {
                // entity.getParent and .getApplicationId not available yet; but spec.getParent does have them;
                // we want to show the message before initialization happens
                Object planId = ((EntityInternal) entity).config().getRaw(BrooklynConfigKeys.PLAN_ID).orNull();
                String name = entity.getId() + " " +
                        (planId != null ? planId + " / " : "") +
                        entity.getDisplayName() + " " +
                        "(" + entity + ")";
                if (spec.getParent() == null) {
                    BrooklynLoggingCategories.APPLICATION_LIFECYCLE_LOG.debug("Creating application " + name +
                            " for user " + Entitlements.getEntitlementContextUser());
                } else {
                    BrooklynLoggingCategories.ENTITY_LIFECYCLE_LOG.debug("Creating entity " + name +
                            " for user " + Entitlements.getEntitlementContextUser() + ", "+
                            "child of " +
                            (!Objects.equals(spec.getParent().getId(), spec.getParent().getApplicationId())
                                    ? "entity " + spec.getParent().getId() + " in "
                                    : "") +
                            "application " + spec.getParent().getApplicationId()
                            );
                }
            }

            loadUnitializedEntity(entity, spec, options);

            if (!options.isDryRun()) {
                // ID's might have only been set above; log again so we can see them
                Object planId = ((EntityInternal) entity).config().getLocalRaw(BrooklynConfigKeys.PLAN_ID).orNull();
                String name = entity.getId() + " " +
                        (planId != null ? planId + " / " : "") +
                        entity.getDisplayName() + " " +
                        "(" + entity + ")";

                if (spec.getParent() == null) {
                    BrooklynLoggingCategories.APPLICATION_LIFECYCLE_LOG.debug("Creation configuration done for application " + name);
                } else {
                    BrooklynLoggingCategories.ENTITY_LIFECYCLE_LOG.debug("Creation configuration done for entity " + name);
                }
            }

            List<NamedStringTag> upgradedFrom = BrooklynTags.findAllNamedStringTags(BrooklynTags.UPGRADED_FROM, spec.getTags());
            if (!upgradedFrom.isEmpty()) {
                log.warn("Entity "+entity.getId()+" created with upgraded type "+entity.getCatalogItemId()+" "+upgradedFrom+" (in "+entity.getApplicationId()+", under "+entity.getParent()+")");
            }

            entitiesByEntityId.put(entity.getId(), entity);
            specsByEntityId.put(entity.getId(), spec);

            for (EntitySpec<?> childSpec : spec.getChildren()) {
                if (childSpec.getParent()!=null) {
                    if (!childSpec.getParent().equals(entity)) {
                        throw new IllegalStateException("Spec "+childSpec+" has parent "+childSpec.getParent()+" defined, "
                            + "but it is defined as a child of "+entity);
                    }
                    log.warn("Child spec "+childSpec+" is already set with parent "+entity+"; how did this happen?!");
                }
                childSpec.parent(entity);
                Entity child = createEntityAndDescendantsUninitialized(depth+1, childSpec, options, entitiesByEntityId, specsByEntityId);
                if (Entities.isUnmanagingOrNoLongerManaged(entity))
                    throw new IllegalStateException("Cannot create "+child+" as child of "+entity+" because the latter is unmanaging or no longer managed");
                entity.addChild(child);
            }

            for (Entity member: spec.getMembers()) {
                if (!(entity instanceof Group)) {
                    throw new IllegalStateException("Entity "+entity+" must be a group to add members "+spec.getMembers());
                }
                ((Group)entity).addMember(member);
            }

            for (Group group : spec.getGroups()) {
                group.addMember(entity);
            }

            return entity;

        } catch (Exception ex) {
            options.onException(ex, Exceptions::propagate);
            return entity;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected <T extends Entity> T loadUnitializedEntity(final T entity, final EntitySpec<T> spec, EntityManager.
        EntityCreationOptions options) {
        try {
            Task<T> initialize = Tasks.create("Initialize model classes", () -> {
                final AbstractEntity theEntity = (AbstractEntity) entity;
                if (spec.getDisplayName() != null)
                    theEntity.setDisplayName(spec.getDisplayName());

                if (spec.getCatalogItemId() != null) {
                    theEntity.setCatalogItemIdAndSearchPath(spec.getCatalogItemId(), spec.getCatalogItemIdSearchPath());
                } else {
                    theEntity.addSearchPath(spec.getCatalogItemIdSearchPath());
                }

                entity.tags().addTags(spec.getTags());
                addSpecParameters(spec, theEntity.getMutableEntityType());

                Map<String, ?> flags = MutableMap.copyOf(spec.getFlags());
                Object extraTags = flags.remove("tags");
                if (extraTags!=null) {
                    theEntity.tags().addTags(TypeCoercions.coerce(extraTags, Iterable.class));
                }
                theEntity.configure(flags);
                for (Entry<ConfigKey<?>, Object> entry : spec.getConfig().entrySet()) {
                    entity.config().set((ConfigKey) entry.getKey(), entry.getValue());
                }

                Entity parent = spec.getParent();
                if (parent != null) {
                    parent = (parent instanceof AbstractEntity) ? ((AbstractEntity) parent).getProxyIfAvailable() : parent;
                    // below will throw if parent is unmanaging, _after_ adding
                    entity.setParent(parent);
                }
                return entity;
            });
//            BrooklynTaskTags.setTransient(initialize);  // don't set this transient; we might want to be able to see what it does, eg adding scheduled tasks
            return ((AbstractEntity) entity).getExecutionContext().get(initialize);

        } catch (Exception e) {
            options.onException(e, Exceptions::propagate);
            return entity;
        }
    }

    private void addSpecParameters(EntitySpec<?> spec, EntityDynamicType edType) {
        // if coming from a catalog item, parsed by CAMP, then the spec list of parameters is canonical,
        // the parent item has had its config keys set as parameters here with those non-inheritable
        // via type definition removed, so wipe those on the EDT to make sure non-inheritable ones are removed;
        // OTOH if item is blank, it was set as a java type, not inheriting it,
        // and the config keys on the dynamic type are the correct ones to use, and usually there is nothing in spec.parameters,
        // except what is being added programmatically.
        // (this logic could get confused if catalog item ID referred to some runtime-inherited context,
        // but those semantics should no longer be used -- https://issues.apache.org/jira/browse/BROOKLYN-445)
        if (Strings.isNonBlank(spec.getCatalogItemId())) {
            edType.clearConfigKeys();
        }
        for (SpecParameter<?> param : spec.getParameters()) {
            edType.addConfigKey(param.getConfigKey());
            if (param.getSensor()!=null) edType.addSensor(param.getSensor());
        }
    }

    /**
     * Calls {@link ConfigConstraints#assertValid(Entity)} on the given entity and all of
     * its descendants.
     */
    private void validateDescendantConfig(Entity e, EntityManager.EntityCreationOptions options) {
        Queue<Entity> queue = Lists.newLinkedList();
        queue.add(e);
        while (!queue.isEmpty()) {
            Entity e1 = queue.poll();
            try {
                ConfigConstraints.assertValid(e1);
            } catch (ConstraintViolationException ex) {
                options.onException(ex, Exceptions::propagate);
            }
            queue.addAll(e1.getChildren());
        }
    }

    protected <T extends Entity> void initEntityAndDescendants(String entityId, final Map<String,Entity> entitiesByEntityId, final Map<String,EntitySpec<?>> specsByEntityId, EntityManager.EntityCreationOptions options, boolean validationRequired) {
        final Entity entity = entitiesByEntityId.get(entityId);
        final EntitySpec<?> spec = specsByEntityId.get(entityId);

        if (entity==null || spec==null) {
            log.debug("Skipping initialization of "+entityId+" found as child of entity being initialized, "
                + "but this child is not one we created; likely it was created by an initializer, "
                + "and thus it should be already fully initialized.");
            return;
        }

        if (validationRequired) {
            // Validate all config before attempting to manage any entity, if being invoked on a root entity.
            // Do this here rather than in manageRecursive so that rebind is unaffected.
            validateDescendantConfig(entity, options);
        }

        ((EntityInternal)entity).getExecutionContext().get(Tasks.builder().dynamic(false).displayName("Entity initialization")
                // no longer transient because the UI groups these more nicely now
//                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .body(new Runnable() {
            @Override
            public void run() {
                ((AbstractEntity)entity).init();

                for (LocationSpec<?> locationSpec : spec.getLocationSpecs()) {
                    // Would prefer to tag with the Proxy object (((AbstractEntity)entity).getProxy())
                    // but it's not clear how is the tag being serialized. Better make sure that
                    // anything in tags can be serialized and deserialized. For example catalog tags
                    // are already accessible through the REST API.
                    LocationSpec<?> taggedSpec = LocationSpec.create(locationSpec)
                            .tag(BrooklynTags.newOwnerEntityTag(entity.getId()));
                    ((AbstractEntity)entity).addLocations(MutableList.<Location>of(
                        managementContext.getLocationManager().createLocation(taggedSpec)));
                }
                ((AbstractEntity)entity).addLocations(spec.getLocations());

                BiConsumer<String,Runnable> runNowOrLater = (name, runnable) -> {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        Throwable interrupt = Exceptions.getFirstThrowableMatching(e, t -> t instanceof InterruptedException || t instanceof RuntimeInterruptedException || t instanceof ImmediateSupplier.ImmediateValueNotAvailableException);
                        if (interrupt != null) {
                            log.debug("Unable to create " + name + " as part of initializing " + entity + " (will submit deferred): " + e);
                            Entities.submit(entity, Tasks.create(name, runnable));
                        } else {
                            throw Exceptions.propagate(e);
                        }
                    }
                };

                List<EntityInitializer> initializers = Stream.concat(getGlobalDeploymentInitializers().stream(), spec.getInitializers().stream())
                        .collect(Collectors.toList());
                for (EntityInitializer initializer: initializers) {
                    runNowOrLater.accept(""+initializer, () -> initializer.apply((EntityInternal)entity));
                }

                for (EnricherSpec<?> enricherSpec : spec.getEnricherSpecs()) {
                    runNowOrLater.accept(""+enricherSpec, () -> entity.enrichers().add(policyFactory.createEnricher(enricherSpec)));
                }

                for (PolicySpec<?> policySpec : spec.getPolicySpecs()) {
                    runNowOrLater.accept(""+policySpec, () -> entity.policies().add(policyFactory.createPolicy(policySpec)));
                }

                for (Entity child: entity.getChildren()) {
                    // right now descendants are initialized depth-first (see the getUnchecked() call below)
                    // they could be done in parallel, but OTOH initializers should be very quick
                    initEntityAndDescendants(child.getId(), entitiesByEntityId, specsByEntityId, options, false);
                }

                if (entity instanceof EntityPostInitializable) {
                    ((EntityPostInitializable)entity).postInit();
                }

            }
        }).build());
    }

    /**
     * Constructs an entity, i.e. instantiate the actual class given a spec,
     * and sets the entity's proxy. Used by this factory to {@link #createEntity(EntitySpec, org.apache.brooklyn.api.mgmt.EntityManager.EntityCreationOptions)}
     * and also used during rebind.
     * <p>
     * If the entityId is provided, then uses that to override the entity's id,
     * but that behaviour is deprecated.
     * <p>
     * The new-style no-arg constructor is preferred, and
     * configuration from the {@link EntitySpec} is <b>not</b> normally applied,
     * although for old-style entities flags from the spec are passed to the constructor.
     * <p>
     */
    private <T extends Entity> T constructEntity(Class<? extends T> clazz, EntitySpec<T> spec, String entityId) {
        T entity = constructEntityImpl(clazz, spec, null, entityId);
        if (((AbstractEntity)entity).getProxy() == null) ((AbstractEntity)entity).setProxy(createEntityProxy(spec, entity));
        return entity;
    }

    /**
     * Constructs a new-style entity (fails if no no-arg constructor).
     * Sets the entity's id and proxy.
     * <p>
     * For use during rebind.
     */
    // TODO would it be cleaner to have rebind create a spec and deprecate this?
    public <T extends Entity> T constructEntity(Class<T> clazz, Iterable<Class<?>> interfaces, String entityId) {
        if (!isNewStyle(clazz)) {
            throw new IllegalStateException("Cannot construct old-style entity "+clazz);
        }
        checkNotNull(entityId, "entityId");
        checkState(interfaces != null && !Iterables.isEmpty(interfaces), "must have at least one interface for entity %s:%s", clazz, entityId);

        T entity = constructEntityImpl(clazz, null, null, entityId);
        if (((AbstractEntity)entity).getProxy() == null) {
            Entity proxy = managementContext.getEntityManager().getEntity(entity.getId());
            if (proxy==null) {
                // normal case, proxy does not exist
                proxy = createEntityProxy(interfaces, entity);
            } else {
                // only if rebinding to existing; don't create a new proxy, then we have proxy explosion
                // but callers must be careful that the entity's proxy does not yet point to it
            }
            ((AbstractEntity)entity).setProxy(proxy);
        }
        return entity;
    }

    private <T extends Entity> T constructEntityImpl(Class<? extends T> clazz, EntitySpec<?> optionalSpec,
            Map<String, ?> optionalConstructorFlags, String entityId) {
        T entity = construct(clazz, optionalSpec, optionalConstructorFlags);

        if (entityId!=null) {
            FlagUtils.setFieldsFromFlags(ImmutableMap.of("id", entityId), entity);
        }
        if (entity instanceof AbstractApplication) {
            FlagUtils.setFieldsFromFlags(ImmutableMap.of("mgmt", managementContext), entity);
        }
        managementContext.prePreManage(entity);
        ((AbstractEntity)entity).setManagementContext(managementContext);

        return entity;
    }

    @Override
    protected <T> T constructOldStyle(Class<T> clazz, Map<String,?> flags) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        if (flags.containsKey("parent") || flags.containsKey("owner")) {
            throw new IllegalArgumentException("Spec's flags must not contain parent or owner; use spec.parent() instead for "+clazz);
        }
        return super.constructOldStyle(clazz, flags);
    }

    private <T extends Entity> Class<? extends T> getImplementedBy(EntitySpec<T> spec) {
        if (spec.getImplementation() != null) {
            return spec.getImplementation();
        } else {
            return entityTypeRegistry.getImplementedBy(spec.getType());
        }
    }

    private List<EntityInitializer> getGlobalDeploymentInitializers() {
        return Arrays.stream(managementContext.getConfig().getConfig(GLOBAL_DEPLOYMENT_INITIALIZER_CLASSNAMES).split(","))
                .filter(Strings::isNonEmpty)
                .map(className -> managementContext.getTypeRegistry()
                        .getMaybe(className, null)
                        .map(registeredType -> (EntityInitializer) managementContext.getTypeRegistry().create(registeredType, null, null))
                        .or(() ->
                                JavaBrooklynClassLoadingContext.create(managementContext)
                                .tryLoadClass(className)
                                .map(aClass -> {
                                    try {
                                        return (EntityInitializer) aClass.newInstance();
                                    } catch (InstantiationException | IllegalAccessException e) {
                                        throw new IllegalStateException(e);
                                    }
                                })
                                .or(() -> {
                                    log.warn("Cannot find initializer '"+className+"'; not in type registry and not found on default classpath; ignoring");
                                    return null;
                                }))
                        )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
