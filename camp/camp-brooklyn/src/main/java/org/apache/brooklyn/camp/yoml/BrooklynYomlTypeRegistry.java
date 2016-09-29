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
package org.apache.brooklyn.camp.yoml;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

/** 
 * Provides a bridge so YOML's type registry can find things in the Brooklyn type registry.
 * <p>
 * There are several subtleties in the loading strategy this registry should use depending
 * when YOML is asking for something to be loaded.  This is done through various
 * {@link RegisteredTypeLoadingContext} instances.  That class is able to do things including:
 * 
 * (a) specify a supertype to use for filtering when finding a type
 * (b) specify a class loading context (eg based on context's catalog item id / definition)
 * (c) specify a set of encountered types to prevent looping and resolve a duplicate name
 *     as a class if it has already been resolved as a YOML item 
 *     (eg yoml item java.pack.Foo declares its type as java.pack.Foo to mean to load it as a class)
 *     or simply bail out if there is a recursive definition (eg require java:java.pack.Foo in the above case)
 * (d) specify a construction instruction specified by wrapper/subtype definitions
 * <p>    
 * Which of these should apply depends on the calling context. The following situations apply:
 * 
 * (1) when YOML makes the first call to resolve the type at "/", we should apply (a) and (b) supplied by the user; 
 *     (c) and (d) should be empty but no harm in applying them (and it will make recursive work easier)
 * (2) if in the course of that call this registry calls to YOML to evaluate a plan definition of a supertype,
 *     it should act like (1) except use the supertype's loader; ie apply everything except (b)
 * (3) if YOML makes a subsequent call to resolve a type at a different path, it should apply only (b),
 *     not any of the others
 * <p>
 * See {@link #getTypeContextFor(YomlContext)} and usages.
 *
 * <p>
 * 
 * More details on library loading.  Consider for instance:
 * 
 * - id: x
 *   item: { type: X }
 * - id: x2
 *   item: { type: x }
 * - id: cluster-x
 *   item: { type: cluster, children: [ { type: x }, { type: X } ] }
 *   
 * And assume these items declare different libraries.
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
public class BrooklynYomlTypeRegistry implements YomlTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(BrooklynYomlTypeRegistry.class);
    
    private static final String JAVA_PREFIX = "java:";
    
    @SuppressWarnings("serial")
    static ConfigKey<List<YomlSerializer>> CACHED_SERIALIZERS = ConfigKeys.newConfigKey(new TypeToken<List<YomlSerializer>>() {}, "yoml.type.serializers",
        "Serializers found for a registered type");

    private ManagementContext mgmt;
    
    private RegisteredTypeLoadingContext rootLoadingContext;

    public BrooklynYomlTypeRegistry(@Nonnull ManagementContext mgmt, @Nonnull RegisteredTypeLoadingContext rootLoadingContext) {
        this.mgmt = mgmt;
        this.rootLoadingContext = rootLoadingContext;
        
    }
    
    protected BrooklynTypeRegistry registry() {
        return mgmt.getTypeRegistry();
    }
    
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml) {
        return newInstanceMaybe(typeName, yoml, rootLoadingContext);
    }
    
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml, @Nonnull YomlContext yomlContextOfThisEvaluation) {
        RegisteredTypeLoadingContext applicableLoadingContext = getTypeContextFor(yomlContextOfThisEvaluation);
        return newInstanceMaybe(typeName, yoml, applicableLoadingContext);
    }

    protected RegisteredTypeLoadingContext getTypeContextFor(YomlContext yomlContextOfThisEvaluation) {
        RegisteredTypeLoadingContext applicableLoadingContext;
        if (Strings.isBlank(yomlContextOfThisEvaluation.getJsonPath())) {
            // at root, use everything
            applicableLoadingContext = rootLoadingContext;
        } else {
            // elsewhere the only thing we apply is the loader
            applicableLoadingContext = RegisteredTypeLoadingContexts.builder().loader(rootLoadingContext.getLoader()).build();
        }
        
        if (yomlContextOfThisEvaluation.getConstructionInstruction()!=null) {
            // also apply any construction instruction we've been given
            applicableLoadingContext = RegisteredTypeLoadingContexts.builder(applicableLoadingContext)
                .constructorInstruction(yomlContextOfThisEvaluation.getConstructionInstruction()).build();
        }
        return applicableLoadingContext;
    }
    
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml, @Nonnull RegisteredTypeLoadingContext typeContext) {
        // yoml may be null, for java type lookups, but we could potentially get rid of that call path
        
        RegisteredType typeR = registry().get(typeName, typeContext);
        
        if (typeR!=null) {
            boolean seenType = typeContext.getAlreadyEncounteredTypes().contains(typeName);
            
            if (!seenType) {
                // instantiate the parent type

                // keep everything (supertype constraint, encountered types, constructor instruction)
                // apart from loader info -- loader should be from the type found here 
                RegisteredTypeLoadingContexts.Builder nextContext = RegisteredTypeLoadingContexts.builder(typeContext);
                nextContext.addEncounteredTypes(typeName);
                // reset the loader (pretty sure this is right -Alex)
                // the create call will attach the loader of typeR
                nextContext.loader(null);
                
                return Maybe.of(registry().create(typeR, nextContext.build(), null));
                
            } else {
                // circular reference means load java, below
            }
        } else {
            // type not found means load java, below
        }
        
        Maybe<Class<?>> t = null;
        
        Exception e = null;
        try {
            t = getJavaTypeInternal(typeName, typeContext);
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
        
        try {
            return ConstructionInstructions.Factory.newDefault(t.get(), typeContext.getConstructorInstruction()).create();
        } catch (Exception e2) {
            return Maybe.absent("Error instantiating type "+typeName+": "+Exceptions.collapseText(e2));
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
    public Maybe<Class<?>> getJavaTypeMaybe(String typeName, YomlContext context) {
        if (typeName==null) return Maybe.absent("null type");
        return getJavaTypeInternal(typeName, getTypeContextFor(context));
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Maybe<Class<?>> maybeClass(Class<?> clazz) {
        // restrict unchecked wildcard generic warning/suppression to here
        return (Maybe) Maybe.of(clazz);
    }
    
    protected Maybe<Class<?>> getJavaTypeInternal(String typeName, RegisteredTypeLoadingContext context) {
        RegisteredType type = registry().get(typeName, context);
        if (type!=null && context!=null && !context.getAlreadyEncounteredTypes().contains(type.getId())) {
            return getJavaTypeInternal(type, context);
        }
        
        // try to load it wrt context
        Class<?> result = null;
        
        // strip generics for the purposes here
        if (typeName.indexOf('<')>0) typeName = typeName.substring(0, typeName.indexOf('<'));
        
        if (result==null) result = Boxing.boxedType(Boxing.getPrimitiveType(typeName).orNull());
        if (result==null && YomlUtils.TYPE_STRING.equals(typeName)) result = String.class;
        
        if (result!=null) return maybeClass(result);
            
        // currently we accept but don't require the 'java:' prefix
        boolean isJava = typeName.startsWith(JAVA_PREFIX);
        typeName = Strings.removeFromStart(typeName, JAVA_PREFIX);
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
            // when looking at supertypes we reset the loader to use loaders for the type,
            // note the traversal in encountered types, and leave the rest (eg supertype restriction) as was
            RegisteredTypeLoadingContext newContext = RegisteredTypeLoadingContexts.builder(context)
                .loader( CatalogUtils.newClassLoadingContext(mgmt, type) )
                .addEncounteredTypes(type.getId())
                .build();

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

    public static Set<String> WARNS = MutableSet.of(
        // don't warn on this base class
        JAVA_PREFIX+Object.class.getName() );
    
    @Override
    public <T> String getTypeNameOfClass(Class<T> type) {
        if (type==null) return null;

        String defaultTypeName = getDefaultTypeNameOfClass(type);
        String cleanedTypeName = Strings.removeFromStart(getDefaultTypeNameOfClass(type), JAVA_PREFIX);
        
        // the code below may be a bottleneck; if so, we should cache or something more efficient
        
        Set<RegisteredType> types = MutableSet.of();
        // look in catalog for something where plan matches and consists only of type
        for (RegisteredType rt: mgmt.getTypeRegistry().getAll()) {
            if (!(rt.getPlan() instanceof YomlTypeImplementationPlan)) continue;
            if (((YomlTypeImplementationPlan)rt.getPlan()).javaType==null) continue;
            if (!((YomlTypeImplementationPlan)rt.getPlan()).javaType.equals(cleanedTypeName)) continue;
            if (rt.getPlan().getPlanData()==null) types.add(rt);
            // are there ever plans we want to permit, eg just defining serializers?
            // (if so check the plan here)
        }
        if (types.size()==1) return Iterables.getOnlyElement(types).getSymbolicName();
        if (types.size()>1) {
            if (WARNS.add(type.getName()))
                log.warn("Multiple registered types for "+type+"; picking one arbitrarily");
            return types.iterator().next().getId();
        }
        
        boolean isJava = defaultTypeName.startsWith(JAVA_PREFIX);
        if (isJava && WARNS.add(type.getName()))
            log.warn("Returning default for type name of "+type+"; catalog entry should be supplied");
        
        return defaultTypeName;
    }
    
    protected <T> String getDefaultTypeNameOfClass(Class<T> type) {
        Maybe<String> primitive = Boxing.getPrimitiveName(type);
        if (primitive.isPresent()) return primitive.get();
        if (String.class.equals(type)) return "string";
        // map and list handled by those serializers
        return JAVA_PREFIX+type.getName();
    }

    @Override
    public Iterable<YomlSerializer> getSerializersForType(String typeName, YomlContext yomlContext) {
        Set<YomlSerializer> result = MutableSet.of();
        // TODO add root loader?
        collectSerializers(typeName, getTypeContextFor(yomlContext), result, MutableSet.of());
        return result;
    }
    
    protected void collectSerializers(Object type, RegisteredTypeLoadingContext context, Collection<YomlSerializer> result, Set<Object> typesVisited) {
        if (type==null) return;
        if (type instanceof String) {
            // convert string to registered type or class 
            Object typeR = registry().get((String)type);
            if (typeR==null) {
                typeR = getJavaTypeInternal((String)type, context).orNull();
            }
            if (typeR==null) {
                // will this ever happen in normal operations?
                log.warn("Could not find '"+type+" when collecting serializers");
                return;
            }
            type = typeR;
        }
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
            result.addAll(new BrooklynYomlAnnotations().findSerializerAnnotations((Class<?>)type, false));
//            // could look up the type? but we should be calling this normally with the RT if we have one so probably not necessary
//            // and could recurse through superclasses and interfaces -- but the above is a better place to do that if needed
//            String name = getTypeNameOfClass((Class<?>)type);
//            if (name.startsWith(JAVA_PREFIX)) {
//                find...
////              supers.add(((Class<?>) type).getSuperclass());
////              supers.addAll(Arrays.asList(((Class<?>) type).getInterfaces()));
//            }
        } else {
            throw new IllegalStateException("Illegal supertype entry "+type+", visiting "+typesVisited);
        }
        for (Object s: supers) {
            RegisteredTypeLoadingContext unconstrainedSupertypeContext = RegisteredTypeLoadingContexts.builder(context)
                .expectedSuperType(null).build();
            collectSerializers(s, unconstrainedSupertypeContext, result, typesVisited);
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
            Iterable<? extends Object> addlSuperTypesAsClassOrRegisteredType,
            Iterable<YomlSerializer> serializers) {
            
        YomlTypeImplementationPlan plan = new YomlTypeImplementationPlan(planData, javaConcreteType, serializers);
        RegisteredType result = kind==RegisteredTypeKind.SPEC ? RegisteredTypes.spec(symbolicName, version, plan) : RegisteredTypes.bean(symbolicName, version, plan);
        RegisteredTypes.addSuperType(result, javaConcreteType);
        RegisteredTypes.addSuperTypes(result, addlSuperTypesAsClassOrRegisteredType);
        return result;
    }

    /** null symbolic name means to take it from annotations or the class name */
    public static RegisteredType newYomlRegisteredType(RegisteredTypeKind kind, @Nullable String symbolicName, String version, Class<?> clazz) {
        Set<String> names = new BrooklynYomlAnnotations().findTypeNamesFromAnnotations(clazz, symbolicName, false);
        
        Set<YomlSerializer> serializers = new BrooklynYomlAnnotations().findSerializerAnnotations(clazz, false);
                
        RegisteredType type = BrooklynYomlTypeRegistry.newYomlRegisteredType(kind, 
            // symbolicName, version, 
            names.iterator().next(), version, 
            // planData - null means just use the java type (could have done this earlier), 
            null, 
            // javaConcreteType, superTypesAsClassOrRegisteredType, serializers)
            clazz, Arrays.asList(clazz), serializers);
        type = RegisteredTypes.addAliases(type, names);
        return type;
    }

}
