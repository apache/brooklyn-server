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
package org.apache.brooklyn.core.typereg;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.mgmt.rebind.mementos.CatalogItemMemento;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer.JavaClassNameTypeImplementationPlan;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.Maybe.Absent;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.VersionComparator;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;

/**
 * Utility and preferred creation mechanisms for working with {@link RegisteredType} instances.
 * <p>
 * Use {@link #bean(String, String, TypeImplementationPlan)} and {@link #spec(String, String, TypeImplementationPlan)}
 * to create {@link RegisteredType} instances.
 * <p>
 * See {@link #isSubtypeOf(RegisteredType, Class)} or {@link #isSubtypeOf(RegisteredType, RegisteredType)} to 
 * inspect the type hierarchy.
 */
public class RegisteredTypes {

    private static final Logger log = LoggerFactory.getLogger(RegisteredTypes.class);
    
    @SuppressWarnings("serial")
    static ConfigKey<Class<?>> ACTUAL_JAVA_TYPE = ConfigKeys.newConfigKey(new TypeToken<Class<?>>() {}, "java.type.actual",
        "The actual Java type which will be instantiated (bean) or pointed at (spec)");

    /** @deprecated since it was introduced in 0.9.0; for backwards compatibility only, may be removed at any point */
    @Deprecated
    static final Function<CatalogItem<?,?>,RegisteredType> CI_TO_RT = new Function<CatalogItem<?,?>, RegisteredType>() {
        @Override
        public RegisteredType apply(CatalogItem<?, ?> item) {
            return of(item);
        }
    };
    
    /** @deprecated since it was introduced in 0.9.0; for backwards compatibility only, may be removed at any point */
    @Deprecated
    public static RegisteredType of(CatalogItem<?, ?> item) {
        if (item==null) return null;
        TypeImplementationPlan impl = null;
        if (item.getPlanYaml()!=null) {
            impl = new BasicTypeImplementationPlan(null, item.getPlanYaml());
        } else if (item.getJavaType()!=null) {
            impl = new JavaClassNameTypeImplementationPlan(item.getJavaType());
        } else {
            throw new IllegalStateException("Unsupported catalog item "+item+" when trying to create RegisteredType");
        }
        
        BasicRegisteredType type = (BasicRegisteredType) spec(item.getSymbolicName(), item.getVersion(), impl, item.getCatalogItemJavaType());
        type.containingBundle = item.getContainingBundle();
        type.displayName = item.getDisplayName();
        type.description = item.getDescription();
        type.iconUrl = item.getIconUrl();
        
        type.disabled = item.isDisabled();
        type.deprecated = item.isDeprecated();
        if (item.getLibraries()!=null) type.bundles.addAll(item.getLibraries());
        // aliases aren't on item
        if (item.tags()!=null) type.tags.addAll(item.tags().getTags());
        if (item.getCatalogItemType()==CatalogItemType.TEMPLATE) {
            type.tags.add(BrooklynTags.CATALOG_TEMPLATE);
        }

        // these things from item we ignore: javaType, specType, registeredTypeName ...
        return type;
    }

    /** @deprecated since introduced in 0.12.0; for backwards compatibility only, may be removed at any point.
     * Returns a partially-populated CatalogItem. Many methods throw {@link UnsupportedOperationException}
     * but the basic ones work. */
    @Deprecated
    public static CatalogItem<?,?> toPartialCatalogItem(RegisteredType t) {
        return new CatalogItemFromRegisteredType(t);
    }
    private static class CatalogItemFromRegisteredType implements CatalogItem<Object,Object> {
        private final RegisteredType type;
        public CatalogItemFromRegisteredType(RegisteredType type) { this.type = type; }
        @Override public String getDisplayName() { return type.getDisplayName(); }
        @Override public String getCatalogItemId() { return type.getVersionedName().toString(); }
        @Override public String getId() { return type.getId(); }
        @Override public String getName() { return type.getSymbolicName(); }
        @Override public String getSymbolicName() { return type.getSymbolicName(); }
        @Override public String getRegisteredTypeName() { return type.getSymbolicName(); }
        @Override public String getDescription() { return type.getDescription(); }
        @Override public String getIconUrl() { return type.getIconUrl(); }
        @Override public String getContainingBundle() { return type.getContainingBundle(); }
        @Override public String getVersion() { return type.getVersion(); }

        @Override public List<String> getCatalogItemIdSearchPath() { throw new UnsupportedOperationException(); }
        @Override public TagSupport tags() { throw new UnsupportedOperationException(); }
        @Override public RelationSupport<?> relations() { throw new UnsupportedOperationException(); }
        @Override public <T> T setConfig(ConfigKey<T> key, T val) { throw new UnsupportedOperationException(); }
        @Override public <T> T getConfig(ConfigKey<T> key) { throw new UnsupportedOperationException(); }
        @Override public ConfigurationSupport config() { throw new UnsupportedOperationException(); }
        @Override public SubscriptionSupport subscriptions() { throw new UnsupportedOperationException(); }
        @Override public org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType getCatalogItemType() { throw new UnsupportedOperationException(); }
        @Override public Class<Object> getCatalogItemJavaType() { throw new UnsupportedOperationException(); }
        @Override public Class<Object> getSpecType() { throw new UnsupportedOperationException(); }
        @Override public String getJavaType() { throw new UnsupportedOperationException(); }
        @Override public Collection<org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle> getLibraries() {
            throw new UnsupportedOperationException();
        }
        @Override public String getPlanYaml() { throw new UnsupportedOperationException(); }
        @Override public RebindSupport<CatalogItemMemento> getRebindSupport() { throw new UnsupportedOperationException(); }
        @Override public void setDeprecated(boolean deprecated) { throw new UnsupportedOperationException(); }
        @Override public void setDisabled(boolean disabled) { throw new UnsupportedOperationException(); }
        @Override public boolean isDeprecated() { throw new UnsupportedOperationException(); }
        @Override public boolean isDisabled() { throw new UnsupportedOperationException(); }
    }
    
    /** Preferred mechanism for defining a bean {@link RegisteredType}. 
     * Callers should also {@link #addSuperTypes(RegisteredType, Iterable)} on the result.*/
    public static RegisteredType bean(@Nonnull String symbolicName, @Nonnull String version, @Nonnull TypeImplementationPlan plan) {
        if (symbolicName==null || version==null) log.warn("Deprecated use of RegisteredTypes API passing null name/version", new Exception("Location of deprecated use, wrt "+plan));
        return new BasicRegisteredType(RegisteredTypeKind.BEAN, symbolicName, version, plan);
    }
    /** Convenience for {@link #bean(String, String, TypeImplementationPlan)} when there is a single known java signature/super type */
    public static RegisteredType bean(@Nonnull String symbolicName, @Nonnull String version, @Nonnull TypeImplementationPlan plan, @Nonnull Class<?> superType) {
        if (superType==null) log.warn("Deprecated use of RegisteredTypes API passing null supertype", new Exception("Location of deprecated use, wrt "+symbolicName+":"+version+" "+plan));
        return addSuperType(bean(symbolicName, version, plan), superType);
    }
    
    /** Preferred mechanism for defining a spec {@link RegisteredType}.
     * Callers should also {@link #addSuperTypes(RegisteredType, Iterable)} on the result.*/
    public static RegisteredType spec(@Nonnull String symbolicName, @Nonnull String version, @Nonnull TypeImplementationPlan plan) {
        if (symbolicName==null || version==null) log.warn("Deprecated use of RegisteredTypes API passing null supertype", new Exception("Location of deprecated use, wrt "+plan));
        return new BasicRegisteredType(RegisteredTypeKind.SPEC, symbolicName, version, plan);
    }
    /** Convenience for {@link #spec(String, String, TypeImplementationPlan)} when there is a single known java signature/super type */
    public static RegisteredType spec(@Nonnull String symbolicName, @Nonnull String version, @Nonnull TypeImplementationPlan plan, @Nonnull Class<?> superType) {
        if (superType==null) log.warn("Deprecated use of RegisteredTypes API passing null supertype", new Exception("Location of deprecated use, wrt "+symbolicName+":"+version+" "+plan));
        return addSuperType(spec(symbolicName, version, plan), superType);
    }
    public static RegisteredType newInstance(@Nonnull RegisteredTypeKind kind, @Nonnull String symbolicName, @Nonnull String version, 
            @Nonnull TypeImplementationPlan plan, @Nonnull Iterable<Object> superTypes,
            Iterable<String> aliases, Iterable<Object> tags,
            String containingBundle, Iterable<OsgiBundleWithUrl> libraryBundles, 
            String displayName, String description, String catalogIconUrl, 
            Boolean catalogDeprecated, Boolean catalogDisabled) {
        BasicRegisteredType result = new BasicRegisteredType(kind, symbolicName, version, plan);
        addSuperTypes(result, superTypes);
        addAliases(result, aliases);
        addTags(result, tags);
        result.containingBundle = containingBundle;
        Iterables.addAll(result.bundles, libraryBundles);
        result.displayName = displayName;
        result.description = description;
        result.iconUrl = catalogIconUrl;
        if (catalogDeprecated!=null) result.deprecated = catalogDeprecated;
        if (catalogDisabled!=null) result.disabled = catalogDisabled;
        return result;
    }
    public static RegisteredType copy(RegisteredType t) {
        return copyResolved(t.getKind(), t);
    }
    @Beta
    public static RegisteredType copyResolved(RegisteredTypeKind kind, RegisteredType t) {
        if (t.getKind()!=null && t.getKind()!=RegisteredTypeKind.UNRESOLVED && t.getKind()!=kind) {
            throw new IllegalStateException("Cannot copy resolve "+t+" ("+t.getKind()+") as "+kind);
        }
        return newInstance(kind, t.getSymbolicName(), t.getVersion(), t.getPlan(), 
            t.getSuperTypes(), t.getAliases(), t.getTags(), t.getContainingBundle(), t.getLibraries(), 
            t.getDisplayName(), t.getDescription(), t.getIconUrl(), t.isDeprecated(), t.isDisabled());
    }

    /** Creates an anonymous {@link RegisteredType} for plan-instantiation-only use. */
    @Beta
    // internal only in *TypeRegistry.create methods; uses null for name and version,
    // which is ignored in the TypePlanTransformer; might be better to clean up the API to avoid this
    public static RegisteredType anonymousRegisteredType(RegisteredTypeKind kind, TypeImplementationPlan plan) {
        return new BasicRegisteredType(kind, null, null, plan);
    }
    
    /** returns the {@link Class} object corresponding to the given java type name and registered type,
     * using the cache on the type in the first instance, falling back to the loader defined the type and context. */
    @Beta
    // TODO should this be on the AbstractTypePlanTransformer ?
    public static Class<?> loadActualJavaType(String javaTypeName, ManagementContext mgmt, RegisteredType type, RegisteredTypeLoadingContext context) {
        Class<?> result = peekActualJavaType(type);
        if (result!=null) return result;
        
        result = CatalogUtils.newClassLoadingContext(mgmt, type, context==null ? null : context.getLoader()).loadClass( javaTypeName );
        
        cacheActualJavaType(type, result);
        return result;
    }
    @Beta public static Class<?> peekActualJavaType(RegisteredType type) {
        return ((BasicRegisteredType)type).getCache().get(ACTUAL_JAVA_TYPE);
    }
    @Beta public static void cacheActualJavaType(RegisteredType type, Class<?> clazz) {
        ((BasicRegisteredType)type).getCache().put(ACTUAL_JAVA_TYPE, clazz);
    }

    @Beta
    public static RegisteredType setContainingBundle(RegisteredType type, @Nullable ManagedBundle bundle) {
        ((BasicRegisteredType)type).containingBundle =
            bundle==null ? null : Strings.toString(bundle.getVersionedName());
        return type;
    }

    @Beta
    public static RegisteredType setDeprecated(RegisteredType type, boolean deprecated) {
        ((BasicRegisteredType)type).deprecated = deprecated;
        return type;
    }

    @Beta
    public static RegisteredType setDisabled(RegisteredType type, boolean disabled) {
        ((BasicRegisteredType)type).disabled = disabled;
        return type;
    }

    @Beta
    public static RegisteredType addSuperType(RegisteredType type, @Nullable Class<?> superType) {
        if (superType!=null) {
            ((BasicRegisteredType)type).superTypes.add(superType);
        }
        return type;
    }
    @Beta
    public static RegisteredType addSuperType(RegisteredType type, @Nullable RegisteredType superType) {
        if (superType!=null) {
            if (isSubtypeOf(superType, type)) {
                throw new IllegalStateException(superType+" declares "+type+" as a supertype; cannot set "+superType+" as a supertype of "+type);
            }
            ((BasicRegisteredType)type).superTypes.add(superType);
        }
        return type;
    }
    @Beta
    public static RegisteredType addSuperTypes(RegisteredType type, Iterable<? extends Object> superTypesAsClassOrRegisteredType) {
        if (superTypesAsClassOrRegisteredType!=null) {
            for (Object superType: superTypesAsClassOrRegisteredType) {
                if (superType==null) {
                    // nothing
                } else if (superType instanceof Class) {
                    addSuperType(type, (Class<?>)superType);
                } else if (superType instanceof RegisteredType) {
                    addSuperType(type, (RegisteredType)superType);
                } else {
                    throw new IllegalStateException(superType+" supplied as a supertype of "+type+" but it is not a supported supertype");
                }
            }
        }
        return type;
    }

    @Beta
    public static RegisteredType addAlias(RegisteredType type, String alias) {
        if (alias!=null) {
            ((BasicRegisteredType)type).aliases.add( alias );
        }
        return type;
    }
    @Beta
    public static RegisteredType addAliases(RegisteredType type, Iterable<String> aliases) {
        if (aliases!=null) {
            for (String alias: aliases) addAlias(type, alias);
        }
        return type;
    }

    @Beta
    public static RegisteredType addTag(RegisteredType type, Object tag) {
        if (tag!=null) {
            ((BasicRegisteredType)type).tags.add( tag );
        }
        return type;
    }
    @Beta
    public static RegisteredType addTags(RegisteredType type, Iterable<?> tags) {
        if (tags!=null) {
            for (Object tag: tags) addTag(type, tag);
        }
        return type;
    }

    /** returns the implementation data for a spec if it is a string (e.g. plan yaml or java class name); else throws */
    @Beta
    public static String getImplementationDataStringForSpec(RegisteredType item) {
        if (item==null || item.getPlan()==null) return null;
        Object data = item.getPlan().getPlanData();
        if (data==null) throw new IllegalStateException("No plan data for "+item);
        if (!(data instanceof String)) throw new IllegalStateException("Expected plan data for "+item+" to be a string");
        return (String)data;
    }

    /** returns an implementation of the spec class corresponding to the given target type;
     * for use in {@link BrooklynTypePlanTransformer#create(RegisteredType, RegisteredTypeLoadingContext)} 
     * implementations when dealing with a spec; returns null if none found
     * @param mgmt */
    @Beta
    public static AbstractBrooklynObjectSpec<?,?> newSpecInstance(ManagementContext mgmt, Class<? extends BrooklynObject> targetType) throws Exception {
        Class<? extends AbstractBrooklynObjectSpec<?, ?>> specType = RegisteredTypeLoadingContexts.lookupSpecTypeForTarget(targetType);
        if (specType==null) return null;
        Method createMethod = specType.getMethod("create", Class.class);
        return (AbstractBrooklynObjectSpec<?, ?>) createMethod.invoke(null, targetType);
    }

    /** Returns a wrapped map, if the object is YAML which parses as a map; 
     * otherwise returns absent capable of throwing an error with more details */
    @SuppressWarnings("unchecked")
    public static Maybe<Map<?,?>> getAsYamlMap(Object planData) {
        if (!(planData instanceof String)) return Maybe.absent("not a string");
        Iterable<Object> result;
        try {
            result = Yamls.parseAll((String)planData);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return Maybe.absent(e);
        }
        Iterator<Object> ri = result.iterator();
        if (!ri.hasNext()) return Maybe.absent("YAML has no elements in it");
        Object r1 = ri.next();
        if (ri.hasNext()) return Maybe.absent("YAML has multiple elements in it");
        if (r1 instanceof Map) return (Maybe<Map<?,?>>)(Maybe<?>) Maybe.of(r1);
        return Maybe.absent("YAML does not contain a map");
    }

    /** 
     * Queries recursively the supertypes of {@link RegisteredType} to see whether it 
     * inherits from the given {@link RegisteredType} */
    public static boolean isSubtypeOf(RegisteredType type, RegisteredType superType) {
        if (type.equals(superType)) return true;
        for (Object st: type.getSuperTypes()) {
            if (st instanceof RegisteredType) {
                if (isSubtypeOf((RegisteredType)st, superType)) return true;
            }
        }
        return false;
    }

    /** 
     * Queries recursively the supertypes of {@link RegisteredType} to see whether it 
     * inherits from the given {@link Class} */
    public static boolean isSubtypeOf(RegisteredType type, Class<?> superType) {
        return isAnyTypeSubtypeOf(type.getSuperTypes(), superType);
    }
    
    /** 
     * Queries recursively the given types (either {@link Class} or {@link RegisteredType}) 
     * to see whether any inherit from the given {@link Class} */
    public static boolean isAnyTypeSubtypeOf(Set<Object> candidateTypes, Class<?> superType) {
        if (superType == Object.class) return true;
        return isAnyTypeOrSuperSatisfying(candidateTypes, Predicates.assignableFrom(superType));
    }

    /** 
     * Queries recursively the given types (either {@link Class} or {@link RegisteredType}) 
     * to see whether any java superclasses satisfy the given {@link Predicate} */
    public static boolean isAnyTypeOrSuperSatisfying(Set<Object> candidateTypes, Predicate<Class<?>> filter) {
        for (Object st: candidateTypes) {
            if (st instanceof Class) {
                if (filter.apply((Class<?>)st)) return true;
            }
        }
        for (Object st: candidateTypes) {
            if (st instanceof RegisteredType) {
                if (isAnyTypeOrSuperSatisfying(((RegisteredType)st).getSuperTypes(), filter)) return true;
            }
        }
        return false;
    }

    /** Validates that the given type matches the context (if supplied);
     * if not satisfied. returns an {@link Absent} if failed with details of the error,
     * with {@link Absent#isNull()} true if the object is null. */
    public static Maybe<RegisteredType> tryValidate(RegisteredType item, final RegisteredTypeLoadingContext constraint) {
        // kept as a Maybe in case someone wants a wrapper around item validity;
        // unclear what the contract should be, as this can return Maybe.Present(null)
        // which is suprising, but it is more natural to callers otherwise they'll likely do a separate null check on the item
        // (often handling null different to errors) so the Maybe.get() is redundant as they have an object for the input anyway.
        
        if (item==null || constraint==null) return Maybe.ofDisallowingNull(item);
        if (constraint.getExpectedKind()!=null && !constraint.getExpectedKind().equals(item.getKind()))
            return Maybe.absent(item+" is not the expected kind "+constraint.getExpectedKind());
        if (constraint.getExpectedJavaSuperType()!=null) {
            if (!isSubtypeOf(item, constraint.getExpectedJavaSuperType())) {
                return Maybe.absent(item+" is not for the expected type "+constraint.getExpectedJavaSuperType());
            }
        }
        return Maybe.of(item);
    }

    /** 
     * Checks whether the given object appears to be an instance of the given registered type */
    private static boolean isSubtypeOf(Class<?> candidate, RegisteredType type) {
        for (Object st: type.getSuperTypes()) {
            if (st instanceof RegisteredType) {
                if (!isSubtypeOf(candidate, (RegisteredType)st)) return false;
            }
            if (st instanceof Class) {
                if (!((Class<?>)st).isAssignableFrom(candidate)) return false;
            }
        }
        return true;
    }

    public static RegisteredType getBestVersion(Iterable<RegisteredType> types) {
        if (types==null || !types.iterator().hasNext()) return null;
        return Ordering.from(RegisteredTypeNameThenBestFirstComparator.INSTANCE).min(types);
    }
    
    /** by name, then with disabled, deprecated first, then by increasing version */ 
    public static class RegisteredTypeNameThenWorstFirstComparator implements Comparator<RegisteredType> {
        public static Comparator<RegisteredType> INSTANCE = new RegisteredTypeNameThenWorstFirstComparator();
        private RegisteredTypeNameThenWorstFirstComparator() {}
        @Override
        public int compare(RegisteredType o1, RegisteredType o2) {
            return ComparisonChain.start()
                .compare(o1.getSymbolicName(), o2.getSymbolicName(), NaturalOrderComparator.INSTANCE)
                .compareTrueFirst(o1.isDisabled(), o2.isDisabled())
                .compareTrueFirst(o1.isDeprecated(), o2.isDeprecated())
                .compare(o1.getVersion(), o2.getVersion(), VersionComparator.INSTANCE)
                .result();
        }
    }

    /** by name, then with disabled, deprecated first, then by increasing version */ 
    public static class RegisteredTypeNameThenBestFirstComparator implements Comparator<RegisteredType> {
        public static Comparator<RegisteredType> INSTANCE = new RegisteredTypeNameThenBestFirstComparator();
        private RegisteredTypeNameThenBestFirstComparator() {}
        @Override
        public int compare(RegisteredType o1, RegisteredType o2) {
            return ComparisonChain.start()
                .compare(o1.getSymbolicName(), o2.getSymbolicName(), NaturalOrderComparator.INSTANCE)
                .compareFalseFirst(o1.isDisabled(), o2.isDisabled())
                .compareFalseFirst(o1.isDeprecated(), o2.isDeprecated())
                .compare(o2.getVersion(), o1.getVersion(), VersionComparator.INSTANCE)
                .result();
        }
    }

    /** validates that the given object (required) satisfies the constraints implied by the given
     * type and context object, using {@link Maybe} as the result set absent containing the error(s)
     * if not satisfied. returns an {@link Absent} if failed with details of the error,
     * with {@link Absent#isNull()} true if the object is null. */
    public static <T> Maybe<T> tryValidate(final T object, @Nullable final RegisteredType type, @Nullable final RegisteredTypeLoadingContext context) {
        if (object==null) return Maybe.absentNull("object is null");
        
        RegisteredTypeKind kind = type!=null ? type.getKind() : context!=null ? context.getExpectedKind() : null;
        if (kind==null) {
            if (object instanceof AbstractBrooklynObjectSpec) kind=RegisteredTypeKind.SPEC;
            else kind=RegisteredTypeKind.BEAN;
        }
        return new RegisteredTypeKindVisitor<Maybe<T>>() {
            @Override
            protected Maybe<T> visitSpec() {
                return tryValidateSpec(object, type, context);
            }

            @Override
            protected Maybe<T> visitBean() {
                return tryValidateBean(object, type, context);
            }
            
            protected Maybe<T> visitUnresolved() { 
                return Maybe.absent(object+" is not yet resolved");
            }
        }.visit(kind);
    }

    private static <T> Maybe<T> tryValidateBean(T object, RegisteredType type, final RegisteredTypeLoadingContext context) {
        if (object==null) return Maybe.absentNull("object is null");
        
        if (type!=null) {
            if (type.getKind()!=RegisteredTypeKind.BEAN)
                return Maybe.absent("Validating a bean when type is "+type.getKind()+" "+type);
            if (!isSubtypeOf(object.getClass(), type))
                return Maybe.absent(object+" does not have all the java supertypes of "+type);
        }

        if (context!=null) {
            if (context.getExpectedKind()!=null && context.getExpectedKind()!=RegisteredTypeKind.BEAN)
                return Maybe.absent("Validating a bean when constraint expected "+context.getExpectedKind());
            if (context.getExpectedJavaSuperType()!=null && !context.getExpectedJavaSuperType().isInstance(object))
                return Maybe.absent(object+" is not of the expected java supertype "+context.getExpectedJavaSuperType());
        }
        
        return Maybe.of(object);
    }

    private static <T> Maybe<T> tryValidateSpec(T object, RegisteredType rType, final RegisteredTypeLoadingContext constraint) {
        if (object==null) return Maybe.absentNull("object is null");
        
        if (!(object instanceof AbstractBrooklynObjectSpec)) {
            Maybe.absent("Found "+object+" when expecting a spec");
        }
        Class<?> targetType = ((AbstractBrooklynObjectSpec<?,?>)object).getType();
        
        if (targetType==null) {
            Maybe.absent("Spec "+object+" does not have a target type");
        }
        
        if (rType!=null) {
            if (rType.getKind()!=RegisteredTypeKind.SPEC)
                Maybe.absent("Validating a spec when type is "+rType.getKind()+" "+rType);
            if (!isSubtypeOf(targetType, rType))
                Maybe.absent(object+" does not have all the java supertypes of "+rType);
        }

        if (constraint!=null) {
            if (constraint.getExpectedJavaSuperType()!=null) {
                if (!constraint.getExpectedJavaSuperType().isAssignableFrom(targetType)) {
                    Maybe.absent(object+" does not target the expected java supertype "+constraint.getExpectedJavaSuperType());
                }
                if (constraint.getExpectedJavaSuperType().isAssignableFrom(BrooklynObjectInternal.class)) {
                    // don't check spec type; any spec is acceptable
                } else {
                    @SuppressWarnings("unchecked")
                    Class<? extends AbstractBrooklynObjectSpec<?, ?>> specType = RegisteredTypeLoadingContexts.lookupSpecTypeForTarget( (Class<? extends BrooklynObject>) constraint.getExpectedJavaSuperType());
                    if (specType==null) {
                        // means a problem in our classification of spec types!
                        Maybe.absent(object+" is returned as spec for unexpected java supertype "+constraint.getExpectedJavaSuperType());
                    }
                    if (!specType.isAssignableFrom(object.getClass())) {
                        Maybe.absent(object+" is not a spec of the expected java supertype "+constraint.getExpectedJavaSuperType());
                    }
                }
            }
        }
        return Maybe.of(object);
    }

    public static String getIconUrl(BrooklynObject object) {
        if (object==null) return null;
        
        NamedStringTag fromTag = BrooklynTags.findFirst(BrooklynTags.ICON_URL, object.tags().getTags());
        if (fromTag!=null) return fromTag.getContents();
        
        ManagementContext mgmt = ((BrooklynObjectInternal)object).getManagementContext();
        if (mgmt==null) return null;
        BrooklynTypeRegistry registry = mgmt.getTypeRegistry();
        if (registry==null) return null;
        RegisteredType item = registry.get( object.getCatalogItemId() );
        if (item==null) return null;
        return item.getIconUrl();
    }

    /** @deprecated since 0.12.0 see {@link #changePlanNotingEquivalent(RegisteredType, TypeImplementationPlan)} */
    @Deprecated
    public static RegisteredType changePlan(RegisteredType type, TypeImplementationPlan plan) {
        ((BasicRegisteredType)type).implementationPlan = plan;
        return type;
    }
    
    /** Changes the plan set on the given type, returning it,
     * and also recording that comparisons checking {@link #arePlansEquivalent(RegisteredType, RegisteredType)}
     * should consider the two plans equivalent.
     */
    @Beta  // would prefer not to need this, but currently it is needed due to how resolver rewrites plan
           // and we then check plan equality when considering a re-install, else we spuriously fail on
           // identical re-installs (eg with dns-etc-hosts-generator)
    public static RegisteredType changePlanNotingEquivalent(RegisteredType type, TypeImplementationPlan plan) {
        RegisteredTypes.notePlanEquivalentToThis(type, type.getPlan());
        RegisteredTypes.notePlanEquivalentToThis(type, plan);
        ((BasicRegisteredType)type).implementationPlan = plan;
        return type;
    }
    
    private static String tagForEquivalentPlan(String input) {
        // plans may be trimmed by yaml parser so do that before checking equivalence
        // it does mean a format change will be ignored
        return "equivalent-plan("+Streams.getMd5Checksum(Streams.newInputStreamWithContents(input.trim()))+")";
    }
    
    @Beta
    public static void notePlanEquivalentToThis(RegisteredType type, TypeImplementationPlan plan) {
        Object data = plan.getPlanData();
        if (data==null) throw new IllegalStateException("No plan data for "+plan+" noted equivalent to "+type);
        if (!(data instanceof String)) throw new IllegalStateException("Expected plan for equivalent to "+type+" to be a string; was "+data);
        ((BasicRegisteredType)type).tags.add(tagForEquivalentPlan((String)data));
    }

    @Beta
    public static boolean arePlansEquivalent(RegisteredType type1, RegisteredType type2) {
        String plan1 = getImplementationDataStringForSpec(type1);
        String plan2 = getImplementationDataStringForSpec(type2);
        if (Strings.isNonBlank(plan1) && Strings.isNonBlank(plan2)) {
            String p2tag = tagForEquivalentPlan(plan2);
            String p1tag = tagForEquivalentPlan(plan1);
            // allow same plan under trimming,
            // or any recorded tag in either direction
            if (Objects.equal(p1tag, p2tag)) return true;
            if (type1.getTags().contains(p2tag)) return true;
            if (type2.getTags().contains(p1tag)) return true;
        }
        return Objects.equal(type1.getPlan(), type2.getPlan());
    }
    
    public static boolean isTemplate(RegisteredType type) {
        if (type==null) return false;
        return type.getTags().contains(BrooklynTags.CATALOG_TEMPLATE);
    }

}
