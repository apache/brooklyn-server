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
package org.apache.brooklyn.core.config.internal;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritances;
import org.apache.brooklyn.config.ConfigInheritances.BasicConfigValueAtContainer;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.config.ConfigMap.ConfigMapWithInheritance;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.config.StructuredConfigKey;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.Maybe.MaybeSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Future;

public abstract class AbstractConfigMapImpl<TContainer extends BrooklynObject> implements ConfigMapWithInheritance<TContainer> {

    /*
     * Changed Sept 2016 so that keys can determine inheritance strategy at every level in the hierarchy,
     * and signifcantly refactored to share code among subclasses, adding Location as a subclass.
     */

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConfigMapImpl.class);

    @Deprecated /** @deprecated since 0.10.0 - see method which uses it */
    protected final transient org.apache.brooklyn.core.entity.internal.ConfigMapViewWithStringKeys mapViewWithStringKeys = new org.apache.brooklyn.core.entity.internal.ConfigMapViewWithStringKeys(this);

    // TODO make final when not working with previously serialized instances
    // (we shouldn't be, but just in case!)
    protected TContainer bo;

    /**
     * Map of configuration information that is defined at start-up time for the entity. These
     * configuration parameters are shared and made accessible to the "children" of this
     * entity.
     * 
     * All iterator accesses (eg copying) should be synchronized.  See {@link #putAllOwnConfigIntoSafely(Map)}.
     */
    protected final Map<ConfigKey<?>,Object> ownConfig;

    protected AbstractConfigMapImpl(TContainer bo) {
        // Not using ConcurrentMap, because want to (continue to) allow null values.
        // Could use ConcurrentMapAcceptingNullVals (with the associated performance hit on entrySet() etc).
        this(bo, Collections.synchronizedMap(new LinkedHashMap<ConfigKey<?>, Object>()));
    }
    protected AbstractConfigMapImpl(TContainer bo, Map<ConfigKey<?>, Object> storage) {
        this.bo = bo;
        this.ownConfig = storage;
    }

    public TContainer getContainer() {
        return bo;
    }

    protected final BrooklynObjectInternal getBrooklynObject() {
        return (BrooklynObjectInternal)bo;
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return getConfigImpl(key, false).getWithoutError().get();
    }
    
    @Override
    public <T> T getConfig(HasConfigKey<T> key) {
        return getConfigImpl(key.getConfigKey(), false).getWithoutError().get();
    }

    @Override
    public Maybe<Object> getConfigLocalRaw(ConfigKey<?> key) {
        return getConfigRaw(key, false);
    }

    protected abstract ExecutionContext getExecutionContext(BrooklynObject bo);
    protected abstract void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue);

    @Override
    public Map<ConfigKey<?>,Object> getAllConfigLocalRaw() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>();
        putAllOwnConfigIntoSafely(result);
        return Collections.unmodifiableMap(result);
    }
    protected Map<ConfigKey<?>, Object> putAllOwnConfigIntoSafely(Map<ConfigKey<?>, Object> result) {
        synchronized (ownConfig) {
            result.putAll(ownConfig);
        }
        return result;
    }
    protected ConfigBag putAllOwnConfigIntoSafely(ConfigBag bag) {
        synchronized (ownConfig) {
            return bag.putAll(ownConfig);
        }
    }

    /** an immutable copy of the config visible at this entity, local and inherited (preferring local) */
    @Override @Deprecated
    public Map<ConfigKey<?>,Object> getAllConfig() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>();
        if (getParent()!=null)
            result.putAll( getParentInternal().config().getInternalConfigMap().getAllConfig() );
        putAllOwnConfigIntoSafely(result);
        return Collections.unmodifiableMap(result);
    }

    /** Creates an immutable copy of the config visible at this entity, local and inherited (preferring local), including those that did not match config keys */
    @Deprecated
    public ConfigBag getAllConfigBag() {
        ConfigBag result = putAllOwnConfigIntoSafely(ConfigBag.newInstance());
        if (getParent()!=null) {
            result.putIfAbsent(
                    ((AbstractConfigMapImpl<?>)getParentInternal().config().getInternalConfigMap()).getAllConfigBag() );
        }
        return result.seal();
    }

    /** As {@link #getLocalConfigRaw()} but in a {@link ConfigBag} for convenience */
    public ConfigBag getLocalConfigBag() {
        return putAllOwnConfigIntoSafely(ConfigBag.newInstance()).seal();
    }

    public Object setConfig(final ConfigKey<?> key, Object v) {
        // Use our own key for writing, (e.g. in-case it should (or should not) be a structured key like MapConfigKey).
        // This is same logic as for getConfig, except we only have to look at our own container.
        ConfigKey<?> ownKey = getKeyAtContainer(getContainer(), key);
        if (ownKey==null) ownKey = key;

        Object val = coerceConfigVal(ownKey, v);
        Object oldVal;
        if (ownKey instanceof StructuredConfigKey) {
            oldVal = ((StructuredConfigKey)ownKey).applyValueToMap(val, ownConfig);
        } else {
            oldVal = ownConfig.put(ownKey, val);
        }
        postSetConfig();
        return oldVal;
    }

    protected abstract void postSetConfig();

    public void setLocalConfig(Map<ConfigKey<?>, ?> vals) {
        synchronized (ownConfig) {
            ownConfig.clear();
            ownConfig.putAll(vals);
        }
    }

    @SuppressWarnings("unchecked")
    public void putAll(Map<?,?> vals) {
        for (Map.Entry<?, ?> entry : vals.entrySet()) {
            if (entry.getKey()==null)
                throw new IllegalArgumentException("Cannot put null key into "+this);
            else if (entry.getKey() instanceof String)
                setConfig(ConfigKeys.newConfigKey(Object.class, (String)entry.getKey()), entry.getValue());
            else if (entry.getKey() instanceof ConfigKey)
                setConfig((ConfigKey<Object>)entry.getKey(), entry.getValue());
            else if (entry.getKey() instanceof HasConfigKey)
                setConfig( ((HasConfigKey<Object>)entry.getKey()).getConfigKey(), entry.getValue() );
            else throw new IllegalArgumentException("Cannot put key "+entry.getKey()+" (unknown type "+entry.getKey().getClass()+") into "+this);
        }
    }

    public void removeKey(String key) {
        ownConfig.remove(ConfigKeys.newConfigKey(Object.class, key));
    }

    public void removeKey(ConfigKey<?> key) {
        ownConfig.remove(key);
    }

    protected final TContainer getParent() {
        return getParentOfContainer(getContainer());
    }

    protected final BrooklynObjectInternal getParentInternal() {
        return (BrooklynObjectInternal) getParent();
    }

    @Override @Deprecated
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited) {
        // does not currently respect inheritance modes
        if (ownConfig.containsKey(key)) {
            return Maybe.of(ownConfig.get(key));
        }
        for (String deprecatedName : key.getDeprecatedNames()) {
            // Unfortunately `config.putAll(map.of(string, val))` (for dynamic config keys, 
            // i.e. where the key is not pre-defined on the entity). Unfortunately that
            // means subsequent lookup must synthesise keys for each deprecated name.
            ConfigKey<?> deprecatedKey = ConfigKeys.newConfigKeyRenamed(deprecatedName, key);
            if (ownConfig.containsKey(deprecatedKey)) {
                LOG.warn("Retrieving value with deprecated config key name '"+deprecatedName+"' for key "+key);
                return Maybe.of(ownConfig.get(deprecatedKey));
            }
        }
        if (!includeInherited || getParent()==null) return Maybe.absent();
        return getParentInternal().config().getInternalConfigMap().getConfigRaw(key, includeInherited);
    }

    protected Object coerceConfigVal(ConfigKey<?> key, Object v) {
        if ((v instanceof Future) || (v instanceof DeferredSupplier) || (v instanceof TaskFactory)) {
            // no coercion for these (coerce on exit)
            return v;
        } else if (key instanceof StructuredConfigKey) {
            // no coercion for these structures (they decide what to do)
            return v;
        } else if ((v instanceof Map || v instanceof Iterable) && key.getType().isInstance(v)) {
            // don't do coercion on put for these, if the key type is compatible, 
            // because that will force resolution deeply
            return v;
        } else {
            try {
                // try to coerce on input, to detect errors sooner
                return TypeCoercions.coerce(v, key.getTypeToken());
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot coerce or set "+v+" to "+key, e);
                // if can't coerce, we could just log as below and *throw* the error when we retrieve the config
                // but for now, fail fast (above), because we haven't encountered strong use cases
                // where we want to do coercion on retrieval, except for the exceptions above
                //                Exceptions.propagateIfFatal(e);
                //                LOG.warn("Cannot coerce or set "+v+" to "+key+" (ignoring): "+e, e);
                //                val = v;
            }
        }
    }

    @Override
    public Map<String,Object> asMapWithStringKeys() {
        return mapViewWithStringKeys;
    }

    @Override
    public int size() {
        return ownConfig.size();
    }

    @Override
    public boolean isEmpty() {
        return ownConfig.isEmpty();
    }

    protected ConfigInheritance getDefaultRuntimeInheritance() {
        return BasicConfigInheritance.OVERWRITE; 
    }

    @Override
    public <T> ReferenceWithError<ConfigValueAtContainer<TContainer,T>> getConfigAndContainer(ConfigKey<T> key) {
        return getConfigImpl(key, false);
    }

    protected abstract TContainer getParentOfContainer(TContainer container);


    @Nullable protected final <T> ConfigKey<T> getKeyAtContainer(TContainer container, ConfigKey<T> queryKey) {
        if (container==null) return null;
        @SuppressWarnings("unchecked")
        ConfigKey<T> candidate = (ConfigKey<T>) getKeyAtContainerImpl(container, queryKey);
        return candidate;
    }

    @Nullable protected abstract <T> ConfigKey<?> getKeyAtContainerImpl(@Nonnull TContainer container, ConfigKey<T> queryKey);
    protected abstract Collection<ConfigKey<?>> getKeysAtContainer(@Nonnull TContainer container);

    protected Maybe<Object> getRawValueAtContainer(TContainer container, ConfigKey<? extends Object> configKey) {
        return ((BrooklynObjectInternal)container).config().getInternalConfigMap().getConfigLocalRaw(configKey);
    }
    /** finds the value at the given container/key, taking in to account any resolution expected by the key (eg for map keys).
     * the input is the value in the {@link #ownConfig} map taken from {@link #getRawValueAtContainer(BrooklynObject, ConfigKey)}, 
     * but the key may have other plans.
     * current impl just uses the key to extract again which is a little bit wasteful but simpler. 
     * <p>
     * this does not do any resolution with respect to ancestors. */
    protected Maybe<Object> resolveRawValueFromContainer(TContainer container, ConfigKey<?> key, Maybe<Object> value) {
        Maybe<Object> result = resolveRawValueFromContainerIgnoringDeprecatedNames(container, key, value);
        if (result.isPresent()) return result;
        
        // See AbstractconfigMapImpl.getConfigRaw(ConfigKey<?> key, boolean includeInherited) for how/why it
        // handles deprecated names
        for (String deprecatedName : key.getDeprecatedNames()) {
            ConfigKey<?> deprecatedKey = ConfigKeys.newConfigKeyRenamed(deprecatedName, key);
            result = resolveRawValueFromContainerIgnoringDeprecatedNames(container, deprecatedKey, value);
            if (result.isPresent()) {
                LOG.warn("Retrieving value with deprecated config key name '"+deprecatedName+"' for key "+key);
                return result;
            }
        }
        return result;
    }
    
    private Maybe<Object> resolveRawValueFromContainerIgnoringDeprecatedNames(TContainer container, ConfigKey<?> key, Maybe<Object> value) {
        Map<ConfigKey<?>, Object> oc = ((AbstractConfigMapImpl<?>) ((BrooklynObjectInternal)container).config().getInternalConfigMap()).ownConfig;
        if (key instanceof ConfigKeySelfExtracting) {
            if (((ConfigKeySelfExtracting<?>)key).isSet(oc)) {
                Map<ConfigKey<?>, ?> ownCopy;
                synchronized (oc) {
                    // wasteful to make a copy to look up; maybe try once opportunistically?
                    ownCopy = MutableMap.copyOf(oc);
                }
                // would be cleaner here to have an extractValueMaybe but semantics can get confusing whether absent
                // means no value can be extracted (getRaw semantics) and immediate mode is on but blocking is needed (ImmediateSupplier semantics);
                // simpler not to support maybe, in which case here null means the former, and the latter throws something (which the caller catches)
                Maybe<Object> result = Maybe.of((Object) ((ConfigKeySelfExtracting<?>) key).extractValue(ownCopy, getExecutionContext(container)) );
                postLocalEvaluate(key, bo, value, result);
                return result;
            } else {
                return Maybe.absent();
            }
        } else {
            // all our keys are self-extracting
            LOG.warn("Unexpected key type "+key+" ("+key.getClass()+") in "+bo+"; ignoring value");
            return Maybe.absent();
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T coerceConfigValue(TContainer container, String name, Object value, TypeToken<T> type) {
        if (type==null || value==null) return (T) value;
        ExecutionContext exec = getExecutionContext(container);
        try {
            T result = Tasks.resolveDeepValueCoerced(value, type, exec, "config "+name); // entity should be in context, and entity toString might be disallowed (during initial validation)

            // best effort to preserve/enforce immutability for defaults
            if (result instanceof Map) return (T) Collections.unmodifiableMap((Map<?,?>)result);
            if (result instanceof List) return (T) Collections.unmodifiableList((List<?>)result);
            if (result instanceof Set) return (T) Collections.unmodifiableSet((Set<?>)result);
            if (result instanceof Collection) return (T) Collections.unmodifiableCollection((Collection<?>)result);
            
            return result;
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Error coercing " + container + "->" + name, e);
        }
    }

    protected <T> ReferenceWithError<ConfigValueAtContainer<TContainer,T>> getConfigImpl(final ConfigKey<T> queryKey, final boolean raw) {
        if (queryKey==null) {
            return ReferenceWithError.newInstanceThrowingError(new BasicConfigValueAtContainer<TContainer,T>(getContainer(), null, null, false,
                    null),
                    new NullPointerException("Query key cannot be null"));
        }
    
        // In case this entity class has overridden the given key (e.g. to set default), then retrieve this entity's key
        Function<TContainer, ConfigKey<T>> keyFn = new Function<TContainer, ConfigKey<T>>() {
            @Override public ConfigKey<T> apply(TContainer input) {
                // should return null if the key is not known, to indicate selected inheritance rules from base key should take effect
                return getKeyAtContainer(input, queryKey);
            }
        };
        ConfigKey<T> ownKey1 = keyFn.apply(getContainer());
        if (ownKey1==null) ownKey1 = queryKey;
        final ConfigKey<T> ownKey = ownKey1;
        @SuppressWarnings("unchecked")
        // NB: can't use ""+getContainerImpl() as this can loop in the case of locations looking up ports
        final TypeToken<T> type = (TypeToken<T>) moreSpecificOrWarningPreferringFirst(ownKey, queryKey, ""+getContainer().getId()+"["+getContainer().getDisplayName()+"]");

        // takes type of own key (or query key if own key not available)
        // takes default of own key if available and has default, else of query key

        Function<Maybe<Object>, Maybe<T>> coerceFn = new Function<Maybe<Object>, Maybe<T>>() {
            @SuppressWarnings("unchecked") @Override public Maybe<T> apply(Maybe<Object> input) {
                if (raw || input==null || input.isAbsent()) return (Maybe<T>)input;
                // use lambda to defer execution if default value not needed.
                // this coercion should never be persisted so this is safe.
                return new MaybeSupplier<T>(() -> (coerceConfigValue(getContainer(), ownKey.getName(), input.get(), type)));
            }
        };
        // prefer default and type of ownKey
        Maybe<T> defaultValue = raw ? Maybe.<T>absent() :
            ownKey.hasDefaultValue() ? coerceFn.apply(Maybe.of((Object)ownKey.getDefaultValue())) : 
                queryKey.hasDefaultValue() ? coerceFn.apply(Maybe.of((Object)queryKey.getDefaultValue())) :
                    Maybe.<T>absent();

        if (ownKey instanceof ConfigKeySelfExtracting) {

            Function<TContainer, Maybe<Object>> lookupFn = new Function<TContainer, Maybe<Object>>() {
                @Override public Maybe<Object> apply(TContainer input) {
                    // lookup against ownKey as it may do extra resolution (eg grab *.* subkeys if a map)
                    Maybe<Object> result = getRawValueAtContainer(input, ownKey);
                    if (!raw) result = resolveRawValueFromContainer(input, ownKey, result);
                    return result;
                }
            };
            Function<TContainer, TContainer> parentFn = new Function<TContainer, TContainer>() {
                @Override public TContainer apply(TContainer input) {
                    return getParentOfContainer(input);
                }
            };
            AncestorContainerAndKeyValueIterator<TContainer, T> ckvi = new AncestorContainerAndKeyValueIterator<TContainer,T>(
                    getContainer(), keyFn, lookupFn, coerceFn, parentFn);

            return ConfigInheritances.resolveInheriting(
                    getContainer(), ownKey, coerceFn.apply(lookupFn.apply(getContainer())), defaultValue, 
                    ckvi, InheritanceContext.RUNTIME_MANAGEMENT, getDefaultRuntimeInheritance());

        } else {
            String message = "Config key "+ownKey+" of "+getBrooklynObject()+" is not a ConfigKeySelfExtracting; cannot retrieve value; returning default";
            LOG.warn(message);
            return ReferenceWithError.newInstanceThrowingError(new BasicConfigValueAtContainer<TContainer,T>(getContainer(), ownKey, null, false,
                    defaultValue),
                    new IllegalStateException(message));
        }
    }

    private static TypeToken<?> moreSpecificOrWarningPreferringFirst(ConfigKey<?> ownKey, ConfigKey<?> queryKey, String context) {
        if (ownKey==null && queryKey==null) return null;
        if (queryKey==null) return ownKey.getTypeToken();
        if (ownKey==null) return queryKey.getTypeToken();
        
        TypeToken<?> ownType = ownKey.getTypeToken();
        TypeToken<?> queryType = queryKey.getTypeToken();
        if (queryType.isAssignableFrom(ownType)) {
            // own type is same or more specific, normal path
            return ownType;
        }
        if (ownType.isAssignableFrom(queryType)) {
            // query type is more specific than type defined; unusual but workable
            LOG.debug("Query for "+queryKey+" wants more specific type than key "+ownKey+" declared on "+context+" (unusual but clear what to do)");
            // previously (to 2017-11) we used the less specific type, only issue noticed was if an anonymous key is persisted
            // ie so a non-declared key before rebind becomes a declared key afterwards.  we're going to fix that also.
            return queryType;
        }
        // types are incompatible - continue previous behaviour of preferring own key, but warn
        LOG.warn("Query for "+queryKey+" on "+context+" matched incompatible declared type in key "+ownKey+"; using the declared type");
        return ownType;
    }
    
    @Override
    public List<ConfigValueAtContainer<TContainer,?>> getConfigAllInheritedRaw(ConfigKey<?> queryKey) {
        List<ConfigValueAtContainer<TContainer, ?>> result = MutableList.of();
        TContainer c = getContainer();
        int count=0;

        final InheritanceContext context = InheritanceContext.RUNTIME_MANAGEMENT;
        ConfigInheritance currentInheritance = ConfigInheritances.findInheritance(queryKey, context, getDefaultRuntimeInheritance());

        BasicConfigValueAtContainer<TContainer, Object> last = null;

        while (c!=null) {
            Maybe<Object> v = getRawValueAtContainer(c, queryKey);
            BasicConfigValueAtContainer<TContainer, Object> next = new BasicConfigValueAtContainer<TContainer, Object>(c, getKeyAtContainer(c, queryKey), v);

            if (last!=null && !currentInheritance.considerParent(last, next, context)) break;

            ConfigInheritance currentInheritanceExplicit = ConfigInheritances.findInheritance(next.getKey(), InheritanceContext.RUNTIME_MANAGEMENT, null);
            if (currentInheritanceExplicit!=null) {
                if (count>0 && !currentInheritanceExplicit.isReinheritable(next, context)) break;
                currentInheritance = currentInheritanceExplicit;
            }

            if (next.isValueExplicitlySet()) result.add(0, next);

            last = next;
            c = getParentOfContainer(c);
            count++;
        }

        return result;
    }

    @Override @Deprecated
    public Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter) {
        return findKeys(filter, KeyFindingMode.PRESENT_NOT_RESOLVED);
    }

    @Override
    public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter) {
        return findKeys(filter, KeyFindingMode.DECLARED_OR_PRESENT);
    }

    @Override
    public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter) {
        return findKeys(filter, KeyFindingMode.PRESENT_AND_RESOLVED);
    }

    protected enum KeyFindingMode { DECLARED_OR_PRESENT, PRESENT_AND_RESOLVED, PRESENT_NOT_RESOLVED }

    @SuppressWarnings("deprecation")
    protected Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter, KeyFindingMode mode) {
        MutableSet<ConfigKey<?>> result = MutableSet.of();
        if (mode==KeyFindingMode.DECLARED_OR_PRESENT) {
            result.addAll( Iterables.filter(getKeysAtContainer(getContainer()), filter) );
        }

        for (ConfigKey<?> k: Iterables.filter(ownConfig.keySet(), filter)) {
            if (result.contains(k)) continue;
            if (mode!=KeyFindingMode.PRESENT_NOT_RESOLVED) {
                ConfigKey<?> k2 = getKeyAtContainer(getContainer(), k);
                if (k2!=null) k = k2;
            }
            result.add(k);
        }

        // due to set semantics local should be added first, it prevents equal items from parent from being added on top
        if (getParent()!=null) {
            // now take from runtime parents, but filtered
            Set<ConfigKey<?>> inherited;
            switch (mode) {
            case DECLARED_OR_PRESENT:  inherited = getParentInternal().config().getInternalConfigMap().findKeysDeclared(filter); break;
            case PRESENT_AND_RESOLVED: inherited = getParentInternal().config().getInternalConfigMap().findKeysPresent(filter); break;
            case PRESENT_NOT_RESOLVED: inherited = getParentInternal().config().getInternalConfigMap().findKeys(filter); break;
            default:
                throw new IllegalStateException("Unsupported key finding mode: "+mode);
            }
            // TODO due to recursive nature we call this N times for the Nth level ancestor 
            result.addAll( filterOutRuntimeNotReinherited(inherited) );
        }
        return result;
    }

    private static Set<ConfigKey<?>> filterOutRuntimeNotReinherited(Set<ConfigKey<?>> inherited) {
        Set<ConfigKey<?>> result = MutableSet.of();
        for (ConfigKey<?> k: inherited) {
            if (ConfigInheritances.isKeyReinheritable(k, InheritanceContext.RUNTIME_MANAGEMENT)) {
                result.add(k);
            }
        }
        return result;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public ReferenceWithError<ConfigValueAtContainer<TContainer,?>> getConfigInheritedRaw(ConfigKey<?> key) {
        return (ReferenceWithError<ConfigValueAtContainer<TContainer,?>>) (ReferenceWithError<?>) getConfigImpl(key, true);
    }

    @Override
    public Map<ConfigKey<?>, Object> getAllConfigInheritedRawValuesIgnoringErrors() {
        Map<ConfigKey<?>, ReferenceWithError<ConfigValueAtContainer<TContainer, ?>>> input = getAllConfigInheritedRawWithErrors();
        Map<ConfigKey<?>, Object> result = MutableMap.of();
        for (Map.Entry<ConfigKey<?>, ReferenceWithError<ConfigValueAtContainer<TContainer, ?>>> pair: input.entrySet()) {
            result.put(pair.getKey(), pair.getValue().getWithoutError().get());
        }
        return result;
    }
    @Override
    public Map<ConfigKey<?>, ReferenceWithError<ConfigValueAtContainer<TContainer, ?>>> getAllConfigInheritedRawWithErrors() {
        return getSelectedConfigInheritedRaw(null, false);
    }

    @Override
    public Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> getAllReinheritableConfigRaw() {
        return getSelectedConfigInheritedRaw(null, true);
    }

    protected Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> getSelectedConfigInheritedRaw(Map<ConfigKey<?>,ConfigKey<?>> knownKeys, boolean onlyReinheritable) {
        Map<ConfigKey<?>, ConfigKey<?>> knownKeysOnType = MutableMap.of();
        for (ConfigKey<?> k: getKeysAtContainer(getContainer())) knownKeysOnType.put(k, k);

        Map<ConfigKey<?>, ConfigKey<?>> knownKeysIncludingDescendants = MutableMap.copyOf(knownKeys);
        knownKeysIncludingDescendants.putAll(knownKeysOnType);

        Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> parents = MutableMap.of();
        if (getParent()!=null) {
            @SuppressWarnings("unchecked")
            Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> po = (Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>>) (Map<?,?>)
            ((AbstractConfigMapImpl<?>)getParentInternal().config().getInternalConfigMap())
            .getSelectedConfigInheritedRaw(knownKeysIncludingDescendants, true);
            parents.putAll(po);
        }

        Map<ConfigKey<?>, Object> local = getAllConfigLocalRaw();

        Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> result = MutableMap.of();

        for (ConfigKey<?> kSet: MutableSet.copyOf(local.keySet()).putAll(parents.keySet())) {
            Maybe<Object> localValue = local.containsKey(kSet) ? Maybe.ofAllowingNull(local.get(kSet)) : Maybe.absent();
            ReferenceWithError<ConfigValueAtContainer<TContainer, ?>> vpr = parents.remove(kSet);

            @SuppressWarnings("unchecked")
            ConfigValueAtContainer<TContainer, Object> vp = vpr==null ? null : (ConfigValueAtContainer<TContainer,Object>) vpr.getWithoutError();

            @Nullable ConfigKey<?> kOnType = knownKeysOnType.get(kSet);
            @Nullable ConfigKey<?> kTypeOrDescendant = knownKeysIncludingDescendants.get(kSet);
            assert kOnType==null || kOnType==kTypeOrDescendant;

            // if no key on type, we must use any descendant declared key here 
            // so that the correct descendant conflict resolution strategy is applied
            ConfigInheritance inhHereOrDesc = ConfigInheritances.findInheritance(kTypeOrDescendant, InheritanceContext.RUNTIME_MANAGEMENT, getDefaultRuntimeInheritance());

            // however for the purpose of qualifying we must not give any key except what is exactly declared here,
            // else reinheritance will be incorrectly deduced
            ConfigValueAtContainer<TContainer,Object> vl = new BasicConfigValueAtContainer<TContainer,Object>(getContainer(), kOnType, localValue);

            ReferenceWithError<ConfigValueAtContainer<TContainer, Object>> vlr = null;
            if (inhHereOrDesc.considerParent(vl, vp, InheritanceContext.RUNTIME_MANAGEMENT)) {
                vlr = inhHereOrDesc.resolveWithParent(vl, vp, InheritanceContext.RUNTIME_MANAGEMENT);
            } else {
                // no need to consider parent, just take vl
                if (!vl.isValueExplicitlySet()) {
                    // inherited parent value NEVER_INHERIT ie overwritten by default value or null here
                    continue;
                }
                vlr = ReferenceWithError.newInstanceWithoutError(vl);
            }
            if (onlyReinheritable) {
                ConfigInheritance inhHere = ConfigInheritances.findInheritance(kOnType, InheritanceContext.RUNTIME_MANAGEMENT, getDefaultRuntimeInheritance());
                if (localValue.isAbsent() && !inhHere.isReinheritable(vl, InheritanceContext.RUNTIME_MANAGEMENT)) {
                    // skip this one
                    continue;
                }
            }
            @SuppressWarnings("unchecked")
            ReferenceWithError<ConfigValueAtContainer<TContainer, ?>> vlro = (ReferenceWithError<ConfigValueAtContainer<TContainer, ?>>) (ReferenceWithError<?>) vlr;
            result.put(kSet, vlro);
        }
        assert parents.isEmpty();

        return result;
    }

    @Override
    public String toString() {
        Map<ConfigKey<?>, Object> sanitizeConfig;
        synchronized (ownConfig) {
            sanitizeConfig = Sanitizer.sanitize(ownConfig);
        }
        return super.toString()+"[local="+sanitizeConfig+"]";
    }

}
