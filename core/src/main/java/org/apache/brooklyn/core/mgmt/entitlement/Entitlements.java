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
package org.apache.brooklyn.core.mgmt.entitlement;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.persist.DeserializingClassRenamesProvider;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

/** @since 0.7.0 */
@Beta
public class Entitlements {

    private static final Logger log = LoggerFactory.getLogger(Entitlements.class);
    
    // ------------------- individual permissions
    
    // TODO applies to bundles and registered types; should pass object or probably better add more entitlements?
    public static EntitlementClass<String> SEE_CATALOG_ITEM = new BasicEntitlementClassDefinition<String>("catalog.see", String.class); 
    public static EntitlementClass<Object> ADD_CATALOG_ITEM = new BasicEntitlementClassDefinition<Object>("catalog.add", Object.class); 
    public static EntitlementClass<StringAndArgument> MODIFY_CATALOG_ITEM = new BasicEntitlementClassDefinition<StringAndArgument>("catalog.modify", StringAndArgument.class); 
    
    public static EntitlementClass<Entity> SEE_ENTITY = new BasicEntitlementClassDefinition<Entity>("entity.see", Entity.class);
    public static EntitlementClass<Entity> RENAME_ENTITY = new BasicEntitlementClassDefinition<Entity>("entity.rename", Entity.class);
    public static EntitlementClass<EntityAndItem<String>> SEE_SENSOR = new BasicEntitlementClassDefinition<EntityAndItem<String>>("sensor.see", EntityAndItem.typeToken(String.class));
    public static EntitlementClass<EntityAndItem<String>> SEE_CONFIG = new BasicEntitlementClassDefinition<EntityAndItem<String>>("config.see", EntityAndItem.typeToken(String.class));
    public static EntitlementClass<TaskAndItem<String>> SEE_ACTIVITY_STREAMS = new BasicEntitlementClassDefinition<TaskAndItem<String>>("activity.streams.see", TaskAndItem.typeToken(String.class));
    // string is effector name; argument may be a map or a list, depending how the args were supplied
    // currently this permission gates even _seeing_ the effector; in future we might have a separate permission for that;
    // this permission also controls setting of _reconfigurable_ config; any other config requires modify entity
    public static EntitlementClass<EntityAndItem<StringAndArgument>> INVOKE_EFFECTOR = new BasicEntitlementClassDefinition<EntityAndItem<StringAndArgument>>("effector.invoke", EntityAndItem.typeToken(StringAndArgument.class));
    public static EntitlementClass<Entity> MODIFY_ENTITY = new BasicEntitlementClassDefinition<Entity>("entity.modify", Entity.class);

    // Adjunct entitlements
    public static EntitlementClass<EntityAdjunct> DELETE_ADJUNCT = new BasicEntitlementClassDefinition<>("adjunct.delete", EntityAdjunct.class);

    // Location entitlements
    public static EntitlementClass<StringAndArgument> ADD_LOCATION = new BasicEntitlementClassDefinition<>("location.add", StringAndArgument.class);
    public static EntitlementClass<StringAndArgument> DELETE_LOCATION = new BasicEntitlementClassDefinition<>("location.delete", StringAndArgument.class);
    public static EntitlementClass<StringAndArgument> SEE_LOCATION = new BasicEntitlementClassDefinition<>("location.see", StringAndArgument.class);

    // Policy entitlements
    public static EntitlementClass<StringAndArgument> ADD_POLICY = new BasicEntitlementClassDefinition<>("policy.add", StringAndArgument.class);
    public static EntitlementClass<Policy> DELETE_POLICY = new BasicEntitlementClassDefinition<>("policy.delete", Policy.class);
    public static EntitlementClass<Policy> START_POLICY = new BasicEntitlementClassDefinition<>("policy.start", Policy.class);
    public static EntitlementClass<Policy> STOP_POLICY = new BasicEntitlementClassDefinition<>("policy.stop", Policy.class);

    public static EntitlementClass<Void> SYSTEM_ADMIN = new BasicEntitlementClassDefinition<>("system.admin", Void.class);
    public static EntitlementClass<Void> HA_STATS = new BasicEntitlementClassDefinition<>("system.ha.stats", Void.class);
    public static EntitlementClass<Void> HA_ADMIN = new BasicEntitlementClassDefinition<>("system.ha.admin", Void.class);
    public static EntitlementClass<Void> SHUTDOWN = new BasicEntitlementClassDefinition<>("system.shutdown", Void.class);
    public static EntitlementClass<Void> USAGE = new BasicEntitlementClassDefinition<>("system.usage", Void.class);



    /**
     * Permission to deploy an application, where parameter is some representation
     * of the app to be deployed (spec instance or yaml plan)
     */
    public static EntitlementClass<Object> DEPLOY_APPLICATION = new BasicEntitlementClassDefinition<Object>("app.deploy", Object.class);
    public static EntitlementClass<Object> ADD_JAVA = new BasicEntitlementClassDefinition<Object>("java.add", Object.class);

    /**
     * Catch-all for catalog, locations, scripting, usage, etc - exporting persistence, shutting down, etc;
     * this is significantly more powerful than {@link #SERVER_STATUS}.
     * NB: this may be refactored and deprecated in future
     */
    public static EntitlementClass<Void> SEE_ALL_SERVER_INFO = new BasicEntitlementClassDefinition<Void>("server.info.all.see", Void.class);

    /**
     * Permission to see general server status info: basically HA status; not nearly as much as {@link #SEE_ALL_SERVER_INFO}
     */
    public static EntitlementClass<Void> SERVER_STATUS = new BasicEntitlementClassDefinition<Void>("server.status", Void.class);
    
    /**
     * Permission to run untrusted code or embedded scripts at the server.
     * A secondary check is required for any operation which could potentially grant root-level access.
     */
    public static EntitlementClass<Void> ROOT = new BasicEntitlementClassDefinition<Void>("root", Void.class);

    /**
     * Permission to query the query the log store
     */
    public static EntitlementClass<Void> LOGBOOK_LOG_STORE_QUERY = new BasicEntitlementClassDefinition<Void>("logbook.query", Void.class);

    /**
     * Permission to execute groovy scripts. Since 1.1 prefer {@link #EXECUTE_SCRIPT}.
     */
    public static EntitlementClass<Void> EXECUTE_GROOVY_SCRIPT = new BasicEntitlementClassDefinition<Void>("groovy_script.execute", Void.class);

    /**
     * Permission to run local script commands, eg 'shell' workflow step, groovy scripts, etc
     */
    public static EntitlementClass<Void> EXECUTE_SCRIPT = new BasicEntitlementClassDefinition<Void>("script.execute", Void.class);

    @SuppressWarnings("unchecked")
    public enum EntitlementClassesEnum {
        ENTITLEMENT_SEE_CATALOG_ITEM(SEE_CATALOG_ITEM) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleSeeCatalogItem((String)argument); } },
        ENTITLEMENT_ADD_CATALOG_ITEM(ADD_CATALOG_ITEM) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleAddCatalogItem(argument); } },
        ENTITLEMENT_MODIFY_CATALOG_ITEM(MODIFY_CATALOG_ITEM) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleModifyCatalogItem((StringAndArgument)argument); } },
        
        ENTITLEMENT_SEE_ENTITY(SEE_ENTITY) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleSeeEntity((Entity)argument); } },
        ENTITLEMENT_SEE_SENSOR(SEE_SENSOR) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleSeeSensor((EntityAndItem<String>)argument); } },
        ENTITLEMENT_INVOKE_EFFECTOR(INVOKE_EFFECTOR) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleInvokeEffector((EntityAndItem<StringAndArgument>)argument); } },
        ENTITLEMENT_MODIFY_ENTITY(MODIFY_ENTITY) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleModifyEntity((Entity)argument); } },
        
        ENTITLEMENT_DEPLOY_APPLICATION(DEPLOY_APPLICATION) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleDeployApplication(argument); } },

        ENTITLEMENT_ADD_JAVA(ADD_JAVA) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleAddJava(argument); } },

        ENTITLEMENT_SEE_ALL_SERVER_INFO(SEE_ALL_SERVER_INFO) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleSeeAllServerInfo(); } },
        ENTITLEMENT_SERVER_STATUS(SERVER_STATUS) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleSeeServerStatus(); } },
        ENTITLEMENT_ROOT(ROOT) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleRoot(); } },
        ENTITLEMENT_EXECUTE_GROOVY_SCRIPT(EXECUTE_GROOVY_SCRIPT) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleExecuteGroovyScript(); } },
        ENTITLEMENT_EXECUTE_SCRIPT(EXECUTE_SCRIPT) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleExecuteScript(); } },

        /* NOTE, 'ROOT' USER ONLY IS ALLOWED TO SEE THE LOGS. */
        ENTITLEMENT_LOGBOOK_QUERY(LOGBOOK_LOG_STORE_QUERY) { public <T> T handle(EntitlementClassesHandler<T> handler, Object argument) { return handler.handleRoot(); } },
        ;
        
        private EntitlementClass<?> entitlementClass;

        EntitlementClassesEnum(EntitlementClass<?> specificClass) {
            this.entitlementClass = specificClass;
        }
        public EntitlementClass<?> getEntitlementClass() {
            return entitlementClass;
        }

        public abstract <T> T handle(EntitlementClassesHandler<T> handler, Object argument);
        
        public static EntitlementClassesEnum of(EntitlementClass<?> entitlementClass) {
            for (EntitlementClassesEnum x: values()) {
                if (entitlementClass.equals(x.getEntitlementClass())) return x;
            }
            return null;
        }
    }
    
    public interface EntitlementClassesHandler<T> {
        public T handleSeeCatalogItem(String catalogItemId);
        public T handleSeeServerStatus();
        public T handleAddCatalogItem(Object catalogItemBeingAdded);
        public T handleModifyCatalogItem(StringAndArgument catalogItemIdAndModification);
        public T handleSeeEntity(Entity entity);
        public T handleSeeSensor(EntityAndItem<String> sensorInfo);
        public T handleInvokeEffector(EntityAndItem<StringAndArgument> effectorInfo);
        public T handleModifyEntity(Entity entity);
        public T handleDeployApplication(Object app);
        public T handleAddJava(Object app);
        public T handleSeeAllServerInfo();
        public T handleExecuteGroovyScript();
        public T handleExecuteScript();
        public T handleRoot();
    }
    
    protected static class Pair<T1,T2> {
        protected final T1 p1;
        protected final T2 p2;
        protected Pair(T1 p1, T2 p2) {
            this.p1 = p1;
            this.p2 = p2;
        }
    }

    public static class EntityAndItem<T> extends Pair<Entity,T> {
        public static <TT> TypeToken<EntityAndItem<TT>> typeToken(Class<TT> type) {
            return new TypeToken<Entitlements.EntityAndItem<TT>>() {
                private static final long serialVersionUID = -738154831809025407L;
            };
        }
        public EntityAndItem(Entity entity, T item) { super (entity, item); }
        public Entity getEntity() { return p1; }
        public T getItem() { return p2; }
        public static <T> EntityAndItem<T> of(Entity entity, T item) {
            return new EntityAndItem<T>(entity, item);
        }
    }
    
    public static class TaskAndItem<T> extends Pair<Task<?>,T> {
        public static <TT> TypeToken<TaskAndItem<TT>> typeToken(Class<TT> type) {
            return new TypeToken<Entitlements.TaskAndItem<TT>>() {
                private static final long serialVersionUID = 3103447462213439135L;
            };
        }
        public TaskAndItem(Task<?> task, T item) { super(task, item); }
        public Task<?> getTask() { return p1; }
        public T getItem() { return p2; }
        public static <T> TaskAndItem<T> of(Task<?> task, T item) {
            return new TaskAndItem<T>(task, item);
        }
    }
    
    public static class StringAndArgument extends Pair<String,Object> {
        public StringAndArgument(String string, Object argument) { super(string, argument); }
        public String getString() { return p1; }
        public Object getArgument() { return p2; }
        public static StringAndArgument of(String string, Object argument) {
            return new StringAndArgument(string, argument);
        }
    }

    /** 
     * These lifecycle operations are currently treated as effectors. This may change in the future.
     * @since 0.7.0 */
    @Beta
    public static class LifecycleEffectors {
        public static final String DELETE = "delete";
    }
    
    // ------------- permission sets -------------
    
    /**
     * @return An entitlement manager allowing access to everything.
     */
    public static EntitlementManager root() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T typeArgument) {
                return true;
            }
            @Override
            public String toString() {
                return "Entitlements.root";
            }
        };
    }

    /**
     * @return An entitlement manager allowing everything but {@link #EXECUTE_SCRIPT}, {@link #EXECUTE_GROOVY_SCRIPT}.
     */
    public static EntitlementManager powerUser() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T entitlementClassArgument) {
                return !EXECUTE_GROOVY_SCRIPT.equals(permission) && !EXECUTE_SCRIPT.equals(permission);
            }
            @Override
            public String toString() {
                return "Entitlements.powerUser";
            }
        };
    }

    /**
     * @return An entitlement manager allowing everything but {@link #ROOT}, {@link #LOGBOOK_LOG_STORE_QUERY}, {@link #SEE_ALL_SERVER_INFO},
     * {@link #EXECUTE_GROOVY_SCRIPT}, {@link #EXECUTE_SCRIPT}, {@link #MODIFY_ENTITY}, and {@link #HA_ADMIN}.
     */
    public static EntitlementManager user() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T entitlementClassArgument) {
                return
                        !SEE_ALL_SERVER_INFO.equals(permission) &&
                        !ROOT.equals(permission) &&
                        !LOGBOOK_LOG_STORE_QUERY.equals(permission) &&
                        !EXECUTE_GROOVY_SCRIPT.equals(permission) &&
                        !EXECUTE_SCRIPT.equals(permission) &&
                        !MODIFY_ENTITY.equals(permission) &&
                        !HA_ADMIN.equals(permission);
            }
            @Override
            public String toString() {
                return "Entitlements.user";
            }
        };
    }

    /**
     * @return An entitlement manager per {@link #user()} but also disallowing {@link #ADD_JAVA}
     */
    public static EntitlementManager blueprintAuthor() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T entitlementClassArgument) {
                return user().isEntitled(context, permission, entitlementClassArgument) && !ADD_JAVA.equals(permission);
            }
            @Override
            public String toString() {
                return "Entitlements.user";
            }
        };
    }

    public static EntitlementManager logViewer() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T entitlementClassArgument) {
                return LOGBOOK_LOG_STORE_QUERY.equals(permission);
            }
            @Override
            public String toString() {
                return "Entitlements.logViewer";
            }
        };
    }

    /**
     * @return An entitlement manager denying access to anything that requires entitlements.
     */
    public static EntitlementManager minimal() {
        return new EntitlementManager() {
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T typeArgument) {
                return false;
            }
            @Override
            public String toString() {
                return "Entitlements.minimal";
            }
        };
    }

    public static class FineGrainedEntitlements {
    
        private static final Joiner COMMA_JOINER = Joiner.on(',');

        public static EntitlementManager anyOf(final EntitlementManager... checkers) {
            return anyOf(Arrays.asList(checkers));
        }
        
        public static EntitlementManager anyOf(final Iterable<? extends EntitlementManager> checkers) {
            return new EntitlementManager() {
                @Override
                public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T typeArgument) {
                    for (EntitlementManager checker: checkers)
                        if (checker.isEntitled(context, permission, typeArgument))
                            return true;
                    return false;
                }
                @Override
                public String toString() {
                    return "Entitlements.anyOf(" + COMMA_JOINER.join(checkers) + ")";
                }
            };
        }
        
        public static EntitlementManager allOf(final EntitlementManager... checkers) {
            return allOf(Arrays.asList(checkers));
        }
        
        public static EntitlementManager allOf(final Iterable<? extends EntitlementManager> checkers) {
            return new EntitlementManager() {
                @Override
                public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T typeArgument) {
                    for (EntitlementManager checker: checkers)
                        if (checker.isEntitled(context, permission, typeArgument))
                            return true;
                    return false;
                }
                @Override
                public String toString() {
                    return "Entitlements.allOf(" + COMMA_JOINER.join(checkers) + ")";
                }
            };
        }

        public static <U> EntitlementManager allowing(EntitlementClass<U> permission, Predicate<U> test) {
            return new SinglePermissionEntitlementChecker<U>(permission, test);
        }

        public static <U> EntitlementManager allowing(EntitlementClass<U> permission) {
            return new SinglePermissionEntitlementChecker<U>(permission, Predicates.<U>alwaysTrue());
        }

        public static class SinglePermissionEntitlementChecker<U> implements EntitlementManager {
            final EntitlementClass<U> permission;
            final Predicate<U> test;
            
            protected SinglePermissionEntitlementChecker(EntitlementClass<U> permission, Predicate<U> test) {
                this.permission = permission;
                this.test = test;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> permission, T typeArgument) {
                return Objects.equal(this.permission, permission) && test.apply((U) typeArgument);
            }
            @Override
            public String toString() {
                return "Entitlements.allowing(" + permission + " -> " + test + ")";
            }
        }

        private static class NonSecretPredicate implements Predicate<EntityAndItem<String>> {
            @Override
            public boolean apply(EntityAndItem<String> input) {
                return input != null && !Sanitizer.IS_SECRET_PREDICATE.apply(input.getItem());
            }

            @Override
            public String toString() {
                return "Predicates.nonSecret";
            }
        }

        public static EntitlementManager seeNonSecretSensors() {
            return allowing(SEE_SENSOR, new NonSecretPredicate());
        }
        
        public static EntitlementManager seeNonSecretConfig() {
            return allowing(SEE_CONFIG, new NonSecretPredicate());
        }
    }
    
    /** allow read-only */
    public static EntitlementManager readOnly() {
        return FineGrainedEntitlements.anyOf(
            FineGrainedEntitlements.allowing(SEE_ENTITY),
            FineGrainedEntitlements.allowing(SEE_ACTIVITY_STREAMS),
            FineGrainedEntitlements.allowing(SEE_CATALOG_ITEM),
            FineGrainedEntitlements.allowing(SERVER_STATUS),
            FineGrainedEntitlements.allowing(SEE_LOCATION),
            FineGrainedEntitlements.allowing(HA_STATS),
            FineGrainedEntitlements.seeNonSecretSensors(),
            FineGrainedEntitlements.seeNonSecretConfig()
        );
    }

    /** allow healthcheck */
    public static EntitlementManager serverStatusOnly() {
        return FineGrainedEntitlements.allowing(SERVER_STATUS);
    }

    // ------------- lookup conveniences -------------

    private static class PerThreadEntitlementContextHolder {
        public static final ThreadLocal<EntitlementContext> perThreadEntitlementsContextHolder = new ThreadLocal<EntitlementContext>();
    }

    /** 
     * Finds the currently applicable {@link EntitlementContext} by examining the current thread
     * then by investigating the current task, its submitter, etc. */
    // NOTE: entitlements are propagated to tasks whenever they are created, as tags
    // (see BrooklynTaskTags.tagForEntitlement and BasicExecutionContext.submitInternal).
    // It might be cheaper to only do this lookup, not to propagate as tags, and to ensure
    // all entitlement operations are wrapped in a task at source; but currently we do not
    // do that so we need at least to set entitlement on the outermost task.
    // Setting it on tasks submitted by a task is not strictly necessary (i.e. in BasicExecutionContext)
    // but seems cheap enough, and means checking entitlements is fast, if we choose to do that more often.
    public static EntitlementContext getEntitlementContext() {
        EntitlementContext context;
        context = PerThreadEntitlementContextHolder.perThreadEntitlementsContextHolder.get();
        if (context!=null) return context;
        
        Task<?> task = Tasks.current();
        while (task!=null) {
            context = BrooklynTaskTags.getEntitlement(task);
            if (context!=null) return context;
            task = task.getSubmittedByTask();
        }
        
        // no entitlements set -- assume entitlements not used, or system internal
        return null;
    }

    public static String getEntitlementContextUser() {
        return getEntitlementContextUserMaybe().or("<system>");
    }

    public static Maybe<String> getEntitlementContextUserMaybe() {
        EntitlementContext ctx = getEntitlementContext();
        if (ctx!=null) {
            String user = ctx.user();
            if (Strings.isNonBlank(user)) return Maybe.of(user);
        }
        return Maybe.absent();
    }

    public static void setEntitlementContext(EntitlementContext context) {
        EntitlementContext oldContext = PerThreadEntitlementContextHolder.perThreadEntitlementsContextHolder.get();
        if (oldContext!=null && context!=null) {
            log.warn("Changing entitlement context from "+oldContext+" to "+context+"; context should have been reset or extended, not replaced");
            log.debug("Trace for entitlement context duplicate overwrite", new Throwable("Trace for entitlement context overwrite"));
        }
        PerThreadEntitlementContextHolder.perThreadEntitlementsContextHolder.set(context);
    }
    
    public static void clearEntitlementContext() {
        PerThreadEntitlementContextHolder.perThreadEntitlementsContextHolder.set(null);
    }
    
    public static <T> boolean isEntitled(EntitlementManager checker, EntitlementClass<T> permission, T typeArgument) {
        return checker.isEntitled(getEntitlementContext(), permission, typeArgument);
    }

    /** throws {@link NotEntitledException} if entitlement not available for current {@link #getEntitlementContext()} */
    public static <T> void checkEntitled(EntitlementManager checker, EntitlementClass<T> permission, T typeArgument) {
        if (!isEntitled(checker, permission, typeArgument)) {
            throw new NotEntitledException(getEntitlementContext(), permission, typeArgument);
        }
    }
    
    // ----------------- initialization ----------------

    public final static String ENTITLEMENTS_CONFIG_PREFIX = "brooklyn.entitlements";

    public static final ConfigKey<String> GLOBAL_ENTITLEMENT_MANAGER = ConfigKeys.newStringConfigKey(ENTITLEMENTS_CONFIG_PREFIX + ".global",
        "Class for entitlements in effect globally; "
        + "short names 'minimal', 'readonly', 'user' or 'root' are permitted here, with the default 'root' giving full access to all declared users; "
        + "or supply the name of an "+EntitlementManager.class+" class to instantiate, taking a 1-arg BrooklynProperties constructor or a 0-arg constructor",
        "root");
    
    public static EntitlementManager newManager(ManagementContext mgmt, BrooklynProperties brooklynProperties) {
        return newGlobalManager(mgmt, brooklynProperties);
    }
    private static EntitlementManager newGlobalManager(ManagementContext mgmt, BrooklynProperties brooklynProperties) {
        return load(mgmt, brooklynProperties, brooklynProperties.getConfig(GLOBAL_ENTITLEMENT_MANAGER));
    }
    
    public static EntitlementManager load(@Nullable ManagementContext mgmt, BrooklynProperties brooklynProperties, String type) {
        if ("root".equalsIgnoreCase(type)) {
            return root();
        } else if ("readonly".equalsIgnoreCase(type) || "read_only".equalsIgnoreCase(type)) {
            return readOnly();
        } else if ("minimal".equalsIgnoreCase(type)) {
            return minimal();
        } else if ("user".equalsIgnoreCase(type)) {
            return user();
        } else if ("powerUser".equalsIgnoreCase(type) || "power_user".equalsIgnoreCase(type)) {
            return powerUser();
        } else if ("blueprintAuthor".equalsIgnoreCase(type) || "blueprint_author".equalsIgnoreCase(type)) {
            return blueprintAuthor();
        }else if ("logViewer".equalsIgnoreCase(type) || "log_viewer".equalsIgnoreCase(type) || "log".equalsIgnoreCase(type)) {
            return logViewer();
        }
        if (Strings.isNonBlank(type)) {
            try {
                Class<?> clazz = new ClassLoaderUtils(Entitlements.class, mgmt).loadClass(DeserializingClassRenamesProvider.INSTANCE.findMappedName(type));
                return (EntitlementManager) instantiate(clazz, ImmutableList.of(
                        new Object[] {mgmt, brooklynProperties},
                        new Object[] {mgmt},
                        new Object[] {brooklynProperties},
                        new Object[0]));
            } catch (Exception e) { 
                throw Exceptions.propagate(e); 
            }
        }
        throw new IllegalStateException("Invalid entitlement manager specified: '"+type+"'");
    }
    
    private static Object instantiate(Class<?> clazz, List<Object[]> constructorArgOptions) {
        try {
            for (Object[] constructorArgOption : constructorArgOptions) {
                Maybe<?> result = Reflections.invokeConstructorFromArgs(clazz, constructorArgOption);
                if (result.isPresent()) return result.get();
            }
        } catch (Exception e) { 
            throw Exceptions.propagate(e); 
        }
        throw new IllegalStateException("No matching constructor to instantiate "+clazz);
    }
}
