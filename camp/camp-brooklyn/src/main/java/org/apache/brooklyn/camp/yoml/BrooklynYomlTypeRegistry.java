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
 */package org.apache.brooklyn.camp.yoml;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.camp.yoml.YomlTypePlanTransformer.YomlTypeImplementationPlan;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.typereg.BasicRegisteredType;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

/** 
 * Provides a bridge so YOML can find things in the Brooklyn type registry.
 */
public class BrooklynYomlTypeRegistry implements YomlTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(BrooklynYomlTypeRegistry.class);
    
    @SuppressWarnings("serial")
    static ConfigKey<List<YomlSerializer>> CACHED_SERIALIZERS = ConfigKeys.newConfigKey(new TypeToken<List<YomlSerializer>>() {}, "yoml.type.serializers",
        "Serializers found for a registered type");

    private ManagementContext mgmt;
    
    /*
     * NB, there are a few subtleties here around library loading.  For instance, given:
     * 
     * - id: x
     *   item: { type: X }
     * - id: x2
     *   item: { type: x }
     * - id: cluster-x
     *   item: { type: cluster, children: [ { type: x }, { type: X } ] }
     *   
     * Assume these items declare different libraries.
     * 
     * We *need* libraries to be used when resolving the reference to a parent java type (e.g. x's parent type X).
     * 
     * We *don't* want libraries to be used transitively, e.g. when x2 or cluster-x refers to x, 
     * only x's libraries apply to loading X, not x2's libraries.
     * 
     * We *may* want libraries to be used when resolving references to types in a plan besides the parent type;
     * e.g. cluster-x's libraries will be needed when resolving its reference to it's child of declared type X.
     * But we might *NOT* want to support that, and could instead require that any type referenced elsewhere
     * be defined as an explicit YAML type in the registry. This will simplify our lives as we will force all 
     * java objects to be explicitly defined as a type in the registry. But it means we won't be able to parse
     * current plans so for the moment this is deferred, and we'd want to go through a "warning" cycle when
     * applying. 
     * 
     * (In that last case we could even be stricter and say that any yaml types
     * should have a simple single yaml-java bridge eg using a JavaClassNameTypeImplementationPlan
     * to facilitate reverse lookup.)
     * 
     * The defaultLoadingContext is used for the first and third cases above.
     * The second case is handled by calling back to the registry with a limited context.
     * (Note it will require some work to be able to distinguish between the third case and the first,
     * as we don't currently have that contextual information when methods here are called.)
     *
     * <p>
     * 
     * One bit of ugly remains:
     * 
     * If we install v1 then v2, cluster-x:1 will pick up x:2.
     * We could change this:
     * - by encouraging explicit `type: x:1` in the definition of cluster-x
     * - by allowing `type: x:.` version shorthand, where `.` means the same version as the calling type
     * - by recording locally-preferred types (aka "friends") on a registered type, 
     *   and looking at those friends first; this would be nice in that we could allow private friends
     *   
     * Yoml.read(...) can take a special "top-level type extensions" in order to find friends,
     * and/or a special "top-level libraries" in order to resolve types in the first instance.
     * These would *not* be passed when resolving a found type.
     */
    private RegisteredTypeLoadingContext defaultLoadingContext;

    public BrooklynYomlTypeRegistry(ManagementContext mgmt, RegisteredTypeLoadingContext defaultLoadingContext) {
        this.mgmt = mgmt;
        this.defaultLoadingContext = defaultLoadingContext;
        
    }
    
    protected BrooklynTypeRegistry registry() {
        return mgmt.getTypeRegistry();
    }
    
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml) {
        return newInstanceMaybe(typeName, yoml, defaultLoadingContext);
    }
    
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml, RegisteredTypeLoadingContext context) {
        RegisteredType typeR = registry().get(typeName, context);
        
        if (typeR!=null) {
            RegisteredTypeLoadingContext nextContext = null;
            if (context==null) {
                nextContext = RegisteredTypeLoadingContexts.alreadyEncountered(MutableSet.of(typeName));
            } else {
                if (!context.getAlreadyEncounteredTypes().contains(typeName)) {
                    // we lose any other contextual information, but that seems _good_ since it was used to find the type,
                    // we probably now want to shift to the loading context of that type, e.g. the x2 example above;
                    // we just need to ensure it doesn't try to load a super which is also a sub!
                    nextContext = RegisteredTypeLoadingContexts.alreadyEncountered(context.getAlreadyEncounteredTypes(), typeName);
                }
            }
            if (nextContext==null) {
                // fall through to path below; we have a circular reference, so need to load java instead
            } else {
                return Maybe.of(registry().create(typeR, nextContext, null));
            }
        }
        
        Maybe<Class<?>> t = null;
        {
            Exception e = null;
            try {
                t = getJavaTypeInternal(typeName, context);
            } catch (Exception ee) {
                Exceptions.propagateIfFatal(ee);
                e = ee;
            }
            if (t.isAbsent()) {
                // generally doesn't come here; it gets filtered by getJavaTypeMaybe
                if (e==null) e = ((Maybe.Absent<?>)t).getException();
                return Maybe.absent("Neither the type registry nor the classpath/libraries could load type "+typeName+
                    (e!=null && !Exceptions.isRootBoringClassNotFound(e, typeName) ? ": "+Exceptions.collapseText(e) : ""));
            }
        }
        try {
            return Maybe.of((Object)t.get().newInstance());
        } catch (Exception e) {
            return Maybe.absent("Error instantiating type "+typeName+": "+Exceptions.collapseText(e));
        }
    }

    @Override
    public Object newInstance(String typeN, Yoml yoml) {
        return newInstanceMaybe(typeN, yoml).orNull();
    }

    static class YomlClassNotFoundException extends RuntimeException {
        private static final long serialVersionUID = 5946251753146668070L;
        public YomlClassNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    /*
     * There is also some complexity around the java type and the supertypes.
     * For context, the latter is needed so callers can filter appropriately.
     * The former is needed so that serializers can filter appropriately
     * (the java type is not very extensively used and could be changed to use the supertypes set,
     * but we have the same problem for both).
     * <p>
     * The issue is that when adding to the catalog the caller likely supplies
     * only the yaml, not a statement of supertypes. So we have to try to figure out
     * the supertypes. We can do this (1) on-load, when it is added, or (2) lazy, when it is accessed.
     * We're going to do (1), and persisting the results, which means we instantiate
     * each item once on addition:  this serves as a useful validation, but it does disallow
     * forward references (ie referenced types must be declared first; for "templates" we could skip this), 
     * but we avoid re-instantiation on rebind (and the ordering issues that can arise there).
     * <p> 
     * Option (2) while it has some attractions it makes the API more entangled between RegisteredType and the transformer,
     * and risks odd errors depending when the lazy evaluation occurs vis-a-vis dependent types.
     */
    @Override
    public Maybe<Class<?>> getJavaTypeMaybe(String typeName) {
        return getJavaTypeInternal(typeName, defaultLoadingContext);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Maybe<Class<?>> maybeClass(Class<?> clazz) {
        // restrict unchecked wildcard generic warning/suppression to here
        return (Maybe) Maybe.of(clazz);
    }
    
    protected Maybe<Class<?>> getJavaTypeInternal(String typeName, RegisteredTypeLoadingContext context) {
        RegisteredType type = registry().get(typeName, context);
        if (type!=null && !context.getAlreadyEncounteredTypes().contains(type.getId())) {
            RegisteredTypeLoadingContext newContext = RegisteredTypeLoadingContexts.loaderAlreadyEncountered(context.getLoader(), context.getAlreadyEncounteredTypes(), typeName);
            return getJavaTypeInternal(type, newContext);
        }
        
        // try to load it wrt context
        Class<?> result = null;
        
        // strip generics for the purposes here
        if (typeName.indexOf('<')>0) typeName = typeName.substring(0, typeName.indexOf('<'));
        
        if (result==null) result = Boxing.boxedType(Boxing.getPrimitiveType(typeName).orNull());
        if (result==null && YomlUtils.TYPE_STRING.equals(typeName)) result = String.class;
        
        if (result!=null) return maybeClass(result);
            
        // currently we accept but don't require the 'java:' prefix
        boolean isJava = typeName.startsWith("java:");
        typeName = Strings.removeFromStart(typeName, "java:");
        BrooklynClassLoadingContext loader = null;
        if (context!=null && context.getLoader()!=null) 
            loader = context.getLoader();
        else
            loader = JavaBrooklynClassLoadingContext.create(mgmt);
        
        Maybe<Class<?>> resultM = loader.tryLoadClass(typeName);
        if (resultM.isPresent()) return resultM;
        if (isJava) return resultM;  // if java was explicit, give the java load error
        RuntimeException e = ((Maybe.Absent<?>)resultM).getException();
        return Maybe.absent("Neither the type registry nor the classpath/libraries could find type "+typeName+
            (e!=null && !Exceptions.isRootBoringClassNotFound(e, typeName) ? ": "+Exceptions.collapseText(e) : ""));
    }

    protected Maybe<Class<?>> getJavaTypeInternal(RegisteredType type, RegisteredTypeLoadingContext context) {
        {
            Class<?> result = RegisteredTypes.peekActualJavaType(type);
            if (result!=null) return maybeClass(result);
        }
        
        String declaredPrimarySuperTypeName = null;
        
        if (type.getPlan() instanceof YomlTypeImplementationPlan) {
            declaredPrimarySuperTypeName = ((YomlTypeImplementationPlan)type.getPlan()).javaType;
        }
        if (declaredPrimarySuperTypeName==null) {
            log.warn("Primary java super type not declared for "+type+"; it should have been specified/inferred at definition time; will try to infer it now");
            // first look at plan, for a `type` block
            if (type.getPlan() instanceof YomlTypeImplementationPlan) {
                Maybe<Map<?, ?>> map = RegisteredTypes.getAsYamlMap(type.getPlan().getPlanData());
                if (map.isPresent()) declaredPrimarySuperTypeName = Strings.toString(map.get().get("type"));
            }
        }
        if (declaredPrimarySuperTypeName==null) {
            // could look at declared super types, instantiate, and take the lowest common if found
            // (but the above is recommended and the preloading below sufficient)
        }
        
        Maybe<Class<?>> result = Maybe.absent("Unable to find java supertype for "+type);
        
        if (declaredPrimarySuperTypeName!=null) {
            // if a supertype name was found, use it
            RegisteredTypeLoadingContext newContext = RegisteredTypeLoadingContexts.loaderAlreadyEncountered(
                CatalogUtils.newClassLoadingContext(mgmt, type), context.getAlreadyEncounteredTypes(), type.getId());
            result = getJavaTypeInternal(declaredPrimarySuperTypeName, newContext);
        }
        
        if (result.isAbsent()) {
            RegisteredTypeLoadingContext newContext = RegisteredTypeLoadingContexts.alreadyEncountered(context.getAlreadyEncounteredTypes());
            // failing that, instantiate it (caching it as type object to prevent recursive lookups?)
            log.warn("Preloading required to determine type for "+type);
            Maybe<Object> m = newInstanceMaybe(type.getId(), null, newContext);
            log.info("Preloading completed, got "+m+" for "+type);
            if (m.isPresent()) result = maybeClass(m.get().getClass());
            else {
                // if declared java type, prefer that error, otherwise give error from above
                if (declaredPrimarySuperTypeName==null) {
                    result = Maybe.absent(new IllegalStateException("Unable to find java supertype declared on "+type+" and unable to instantiate", 
                        ((Maybe.Absent<?>)result).getException()));
                }
            }
        }
        
        if (result.isPresent()) {
            RegisteredTypes.cacheActualJavaType(type, result.get());
        }
        return result;
    }

    @Override
    public String getTypeName(Object obj) {
        return getTypeNameOfClass(obj.getClass());
    }

    @Override
    public <T> String getTypeNameOfClass(Class<T> type) {
        if (type==null) return null;
        // TODO reverse lookup??
        // look in catalog for something where plan matches and consists only of type
        return getDefaultTypeNameOfClass(type);
    }
    
    protected <T> String getDefaultTypeNameOfClass(Class<T> type) {
        Maybe<String> primitive = Boxing.getPrimitiveName(type);
        if (primitive.isPresent()) return primitive.get();
        if (String.class.equals(type)) return "string";
        // map and list handled by those serializers
        return "java:"+type.getName();
    }

    @Override
    public Iterable<YomlSerializer> getSerializersForType(String typeName) {
        Set<YomlSerializer> result = MutableSet.of();
        collectSerializers(typeName, result, MutableSet.of());
        return result;
    }
    
    protected void collectSerializers(Object type, Collection<YomlSerializer> result, Set<Object> typesVisited) {
        if (type instanceof String) type = registry().get((String)type);
        if (type==null) return;
        boolean canUpdateCache = typesVisited.isEmpty(); 
        if (!typesVisited.add(type)) return; // already seen
        Set<Object> supers = MutableSet.of();
        if (type instanceof RegisteredType) {
            List<YomlSerializer> serializers = ((BasicRegisteredType)type).getCache().get(CACHED_SERIALIZERS);
            if (serializers!=null) {
                canUpdateCache = false;
                result.addAll(serializers);
            } else {
                // TODO don't cache if it's a snapshot version
                // (also should invalidate any subtypes, but actually any subtypes of snapshots
                // should also be snapshots -- that should be enforced!)
//                if (isSnapshot( ((RegisteredType)type).getVersion() )) updateCache = false;
                
                // apply serializers from this RT
                TypeImplementationPlan plan = ((RegisteredType)type).getPlan();
                if (plan instanceof YomlTypeImplementationPlan) {
                    result.addAll( ((YomlTypeImplementationPlan)plan).serializers );
                }
                
                // loop over supertypes for serializers declared there (unless we introduce an option to suppress/limit)
                supers.addAll(((RegisteredType) type).getSuperTypes());
            }
        } else if (type instanceof Class) {
            // TODO result.addAll( ... ); based on annotations on the java class
            // then do the following if the evaluation above was not recursive
//            supers.add(((Class<?>) type).getSuperclass());
//            supers.addAll(Arrays.asList(((Class<?>) type).getInterfaces()));
        } else {
            throw new IllegalStateException("Illegal supertype entry "+type+", visiting "+typesVisited);
        }
        for (Object s: supers) {
            collectSerializers(s, result, typesVisited);
        }
        if (canUpdateCache) {
            if (type instanceof RegisteredType) {
                ((BasicRegisteredType)type).getCache().put(CACHED_SERIALIZERS, ImmutableList.copyOf(result));
            } else {
                // could use static weak cache map on classes? if so also update above
            }
        }
    }

    public static RegisteredType newYomlRegisteredType(RegisteredTypeKind kind,
            String symbolicName, String version, String planData,
            Class<?> javaConcreteType,
            Iterable<? extends Object> superTypesAsClassOrRegisteredType,
            Iterable<YomlSerializer> serializers) {
            
        YomlTypeImplementationPlan plan = new YomlTypeImplementationPlan(planData, javaConcreteType, serializers);
        RegisteredType result = kind==RegisteredTypeKind.SPEC ? RegisteredTypes.spec(symbolicName, version, plan) : RegisteredTypes.bean(symbolicName, version, plan);
        RegisteredTypes.addSuperTypes(result, superTypesAsClassOrRegisteredType);
        return result;
    }

}
