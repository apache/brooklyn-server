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
package org.apache.brooklyn.core.entity.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritance.ContainerAndValue;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigMap;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.BasicConfigInheritance.AncestorContainerAndKeyValueIterator;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.config.StructuredConfigKey;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityFunctions;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal.ConfigurationSupportInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class EntityConfigMap extends AbstractConfigMapImpl {

    private static final Logger LOG = LoggerFactory.getLogger(EntityConfigMap.class);

    /** entity against which config resolution / task execution will occur */
    private final AbstractEntity entity;
    /** config bag cache for local anonymous key lookup */
    // TODO this could be removed and everything made more like AdjunctConfigMap
    private final ConfigBag ownConfigBag;

    public EntityConfigMap(AbstractEntity entity) {
        // Not using ConcurrentMap, because want to (continue to) allow null values.
        // Could use ConcurrentMapAcceptingNullVals (with the associated performance hit on entrySet() etc).
        this(entity, Collections.synchronizedMap(Maps.<ConfigKey<?>, Object>newLinkedHashMap()));
    }
    
    public EntityConfigMap(AbstractEntity entity, Map<ConfigKey<?>, Object> storage) {
        this.entity = checkNotNull(entity, "entity must be specified");
        this.ownConfig = checkNotNull(storage, "storage map must be specified");
        this.ownConfigBag = ConfigBag.newInstance();
    }
    
    /* These flags are included for documentation.
     * Changes will require testing.
     * Note that trying to get a raw inherited value can be problematic
     * if it is merging resolved values.
     */
    final static boolean RESOLVE_AND_COERCE_EXTRACTING_KEYS = true;
    final static boolean COERCE_DEFAULT_VALUES = true;
    // TODO We're notifying of config-changed because currently persistence needs to know when the
    // attributeWhenReady is complete (so it can persist the result).
    // Long term, we'll just persist tasks properly so the call to onConfigChanged will go!
    final static boolean NOTIFY_ON_TASK_RESOLUTION = true;

    protected static class LocalEvaluateKeyValue<T> implements Function<Entity,Maybe<T>> {
        ConfigKey<T> keyIgnoringInheritance;
        
        public LocalEvaluateKeyValue(ConfigKey<T> keyIgnoringInheritance) {
            this.keyIgnoringInheritance = keyIgnoringInheritance;
        }
        
        @Override
        public Maybe<T> apply(Entity entity) {
            ExecutionContext exec = ((EntityInternal)entity).getExecutionContext();
            ConfigMap configMap = ((ConfigurationSupportInternal)entity.config()).getInternalConfigMap();
            Map<ConfigKey<?>,Object> ownConfig = ((EntityConfigMap)configMap).ownConfig;
            ConfigBag ownConfigBag = ((EntityConfigMap)configMap).ownConfigBag;
            Maybe<Object> rawValue = configMap.getConfigLocalRaw(keyIgnoringInheritance);
            Maybe<T> ownValue;
            
            // Get own value
            if (RESOLVE_AND_COERCE_EXTRACTING_KEYS && keyIgnoringInheritance instanceof ConfigKeySelfExtracting && ((ConfigKeySelfExtracting<T>)keyIgnoringInheritance).isSet(ownConfig)) {
                Map<ConfigKey<?>, ?> ownCopy;
                synchronized (ownConfig) {
                    // TODO wasteful to make a copy to look up; maybe try once opportunistically?
                    ownCopy = MutableMap.copyOf(ownConfig);
                }
                ownValue = Maybe.of(((ConfigKeySelfExtracting<T>) keyIgnoringInheritance).extractValue(ownCopy, exec));
            } else if (ownConfigBag.containsKey(keyIgnoringInheritance)) {
                // TODO configBag.get doesn't handle tasks/attributeWhenReady - it only uses TypeCoercions
                // Precedence ordering has changed; previously we'd prefer an explicit isSet(inheritedConfig)
                // over the localConfigBag.get(key).
                ownValue = Maybe.of(ownConfigBag.get(keyIgnoringInheritance));
            } else {
                ownValue = Maybe.<T>absent();
            }
            
            if (NOTIFY_ON_TASK_RESOLUTION && rawValue.isPresent() && (rawValue.get() instanceof Task)) {
                ((EntityInternal)entity).getManagementSupport().getEntityChangeListener().onConfigChanged(keyIgnoringInheritance);
            }
            
            return ownValue;
        }
    }
    
    public <T> T getConfig(ConfigKey<T> key, T defaultValue) {
        Function<Entity, ConfigKey<T>> keyFn = EntityFunctions.configKeyFinder(key, null);
        
        // In case this entity class has overridden the given key (e.g. to set default), then retrieve this entity's key
        ConfigKey<T> ownKey = keyFn.apply(entity);
        if (ownKey==null) ownKey = key;
        
        LocalEvaluateKeyValue<T> evalFn = new LocalEvaluateKeyValue<T>(ownKey);

        if (ownKey instanceof ConfigKeySelfExtracting) {
            Maybe<T> ownExplicitValue = evalFn.apply(entity);
            
            AncestorContainerAndKeyValueIterator<Entity, T> ckvi = new AncestorContainerAndKeyValueIterator<Entity,T>(
                entity, keyFn, evalFn, EntityFunctions.parent());
            
            ContainerAndValue<T> result = getDefaultRuntimeInheritance().resolveInheriting(ownKey,
                ownExplicitValue, entity,
                ckvi, InheritanceContext.RUNTIME_MANAGEMENT);
        
            if (result.getValue()!=null) return result.getValue();
        } else {
            LOG.warn("Config key {} of {} is not a ConfigKeySelfExtracting; cannot retrieve value; returning default", ownKey, this);
        }
        return COERCE_DEFAULT_VALUES ? TypeCoercions.coerce(defaultValue, key.getTypeToken()) : defaultValue;
    }

    private ConfigInheritance getDefaultRuntimeInheritance() {
        return BasicConfigInheritance.OVERWRITE; 
    }

    @Override
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited) {
        if (ownConfig.containsKey(key)) return Maybe.of(ownConfig.get(key));
        if (!includeInherited || entity.getParent()==null) return Maybe.absent(); 
        return ((ConfigurationSupportInternal)entity.getParent().config()).getRaw(key);
    }
    
    /** an immutable copy of the config visible at this entity, local and inherited (preferring local) */
    // TODO deprecate because key inheritance not respected
    public Map<ConfigKey<?>,Object> getAllConfig() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>();
        if (entity.getParent()!=null)
            result.putAll( ((BrooklynObjectInternal)entity.getParent()).config().getInternalConfigMap().getAllConfig() );
        result.putAll(ownConfig);
        return Collections.unmodifiableMap(result);
    }

    /** an immutable copy of the config defined at this entity, ie not inherited */
    public Map<ConfigKey<?>,Object> getLocalConfig() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>(ownConfig.size());
        result.putAll(ownConfig);
        return Collections.unmodifiableMap(result);
    }
    
    /** Creates an immutable copy of the config visible at this entity, local and inherited (preferring local), including those that did not match config keys */
    // TODO deprecate because key inheritance not respected
    public ConfigBag getAllConfigBag() {
        ConfigBag result = ConfigBag.newInstanceCopying(ownConfigBag)
                .putAll(ownConfig);
        if (entity.getParent()!=null) {
            result.putIfAbsent(
                ((EntityConfigMap) ((BrooklynObjectInternal)entity.getParent()).config().getInternalConfigMap()).getAllConfigBag() );
        }
        return result.seal();
    }

    /** Creates an immutable copy of the config defined at this entity, ie not inherited, including those that did not match config keys */
    public ConfigBag getLocalConfigBag() {
        return ConfigBag.newInstanceCopying(ownConfigBag)
                .putAll(ownConfig)
                .seal();
    }

    @SuppressWarnings("unchecked")
    public Object setConfig(ConfigKey<?> key, Object v) {
        Object val = coerceConfigVal(key, v);
        Object oldVal;
        if (key instanceof StructuredConfigKey) {
            oldVal = ((StructuredConfigKey)key).applyValueToMap(val, ownConfig);
            // TODO ConfigBag does not handle structured config keys; quick fix is to remove (and should also remove any subkeys;
            // as it stands if someone set string a.b.c in the config bag then removed structured key a.b, then got a.b.c they'd get a vale);
            // long term fix is to support structured config keys in ConfigBag, at which point i think we could remove ownConfig altogether
            ownConfigBag.remove(key);
        } else {
            oldVal = ownConfig.put(key, val);
            ownConfigBag.put((ConfigKey<Object>)key, v);
        }
        entity.config().refreshInheritedConfigOfChildren();
        return oldVal;
    }
    
    public void setLocalConfig(Map<ConfigKey<?>, ?> vals) {
        synchronized (ownConfig) {
            ownConfig.clear();
            ownConfigBag.clear();
            ownConfig.putAll(vals);
            ownConfigBag.putAll(vals);
        }
    }
 
    public void addToLocalBag(Map<String,?> vals) {
        ownConfigBag.putAll(vals);
        // TODO quick fix for problem that ownConfig can get out of synch
        ownConfig.putAll(ownConfigBag.getAllConfigAsConfigKeyMap());
    }

    public void removeFromLocalBag(String key) {
        ownConfigBag.remove(key);
        ownConfig.remove(key);
    }

    @Override
    // TODO deprecate or clarify syntax 
    public EntityConfigMap submap(Predicate<ConfigKey<?>> filter) {
        EntityConfigMap m = new EntityConfigMap(entity, Maps.<ConfigKey<?>, Object>newLinkedHashMap());
        synchronized (ownConfig) {
            for (Map.Entry<ConfigKey<?>,Object> entry: ownConfig.entrySet()) {
                if (filter.apply(entry.getKey())) {
                    m.ownConfig.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (entity.getParent()!=null) {
            merge(m, ((EntityConfigMap) ((ConfigurationSupportInternal)entity.getParent().config()).getInternalConfigMap()).submap(filter));
        }
        m.ownConfigBag.putAll(ownConfig);
        return m;
    }

    private void merge(EntityConfigMap local, EntityConfigMap parent) {
        for (ConfigKey<?> k: parent.ownConfig.keySet()) {
            // TODO apply inheritance
            if (!local.ownConfig.containsKey(k)) {
                local.ownConfig.put(k, parent.ownConfig.get(k));
            }
        }
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
