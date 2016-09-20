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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.config.ConfigMap;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.config.StructuredConfigKey;
import org.apache.brooklyn.core.entity.internal.ConfigMapViewWithStringKeys;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public abstract class AbstractConfigMapImpl implements ConfigMap {

    /*
     * Changed Sept 2016 so that keys can determine inheritance strategy at every level in the hierarchy,
     * and signifcantly refactored to share code among subclasses, adding Location as a subclass.
     */

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConfigMapImpl.class);
    
    protected final ConfigMapViewWithStringKeys mapViewWithStringKeys = new ConfigMapViewWithStringKeys(this);

    protected BrooklynObjectInternal bo;
    
    /**
     * Map of configuration information that is defined at start-up time for the entity. These
     * configuration parameters are shared and made accessible to the "children" of this
     * entity.
     */
    protected final Map<ConfigKey<?>,Object> ownConfig;
    
    protected AbstractConfigMapImpl(BrooklynObject bo) {
        // Not using ConcurrentMap, because want to (continue to) allow null values.
        // Could use ConcurrentMapAcceptingNullVals (with the associated performance hit on entrySet() etc).
        this(bo, Collections.synchronizedMap(new LinkedHashMap<ConfigKey<?>, Object>()));
    }
    protected AbstractConfigMapImpl(BrooklynObject bo, Map<ConfigKey<?>, Object> storage) {
        this.bo = (BrooklynObjectInternal) bo;
        this.ownConfig = storage;
    }

    protected BrooklynObjectInternal getBrooklynObject() {
        return bo;
    }
    
    public <T> T getConfig(ConfigKey<T> key) {
        return getConfigImpl(key);
    }
    

    public <T> T getConfig(HasConfigKey<T> key) {
        return getConfigImpl(key.getConfigKey());
    }
    
    @Override
    public Maybe<Object> getConfigLocalRaw(ConfigKey<?> key) {
        return getConfigRaw(key, false);
    }

    protected abstract <T> T getConfigImpl(ConfigKey<T> key);
    
    protected abstract ExecutionContext getExecutionContext(BrooklynObject bo);
    protected abstract void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue);
    
    protected class LocalEvaluateKeyValue<TContainer extends BrooklynObject, TValue> implements Function<TContainer,Maybe<TValue>> {
        ConfigKey<TValue> keyIgnoringInheritance;
        
        public LocalEvaluateKeyValue(ConfigKey<TValue> keyIgnoringInheritance) {
            this.keyIgnoringInheritance = keyIgnoringInheritance;
        }
        
        @Override
        public Maybe<TValue> apply(TContainer bo) {
            ExecutionContext exec = getExecutionContext(bo);
            
            ConfigMap configMap = ((BrooklynObjectInternal)bo).config().getInternalConfigMap();
            Map<ConfigKey<?>,Object> ownConfig = ((AbstractConfigMapImpl)configMap).ownConfig;
            Maybe<Object> rawValue = configMap.getConfigLocalRaw(keyIgnoringInheritance);
            Maybe<TValue> ownValue;
            
            // Get own value
            if (keyIgnoringInheritance instanceof ConfigKeySelfExtracting) {
                if (((ConfigKeySelfExtracting<TValue>)keyIgnoringInheritance).isSet(ownConfig)) {
                    Map<ConfigKey<?>, ?> ownCopy;
                    synchronized (ownConfig) {
                        // wasteful to make a copy to look up; maybe try once opportunistically?
                        ownCopy = MutableMap.copyOf(ownConfig);
                    }
                    ownValue = Maybe.of(((ConfigKeySelfExtracting<TValue>) keyIgnoringInheritance).extractValue(ownCopy, exec));
                } else {
                    ownValue = Maybe.<TValue>absent();
                }
            } else {
                // all our keys are self-extracting
                LOG.warn("Unexpected key type "+keyIgnoringInheritance+" ("+keyIgnoringInheritance.getClass()+") in "+bo+"; ignoring value");
                ownValue = Maybe.<TValue>absent();
            }

            postLocalEvaluate(keyIgnoringInheritance, bo, rawValue, ownValue);
            return ownValue;
        }
    }
    
    /** an immutable copy of the config defined at this entity, ie not inherited */
    public Map<ConfigKey<?>,Object> getLocalConfig() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>(ownConfig.size());
        result.putAll(ownConfig);
        return Collections.unmodifiableMap(result);
    }
    
    /** Creates an immutable copy of the config defined at this entity, ie not inherited, including those that did not match config keys */
    public ConfigBag getLocalConfigBag() {
        return ConfigBag.newInstance().putAll(ownConfig).seal();
    }

    public Object setConfig(ConfigKey<?> key, Object v) {
        Object val = coerceConfigVal(key, v);
        Object oldVal;
        if (key instanceof StructuredConfigKey) {
            oldVal = ((StructuredConfigKey)key).applyValueToMap(val, ownConfig);
        } else {
            oldVal = ownConfig.put(key, val);
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
 
    public void addToLocalBag(Map<String,?> vals) {
//        ConfigBag ownConfigBag = ConfigBag.newInstance().putAll(vals);
//        ownConfig.putAll(ownConfigBag.getAllConfigAsConfigKeyMap());
        // below seems more straightforward; should be the same.
        // potential problem if clash of config key types?
        for (Map.Entry<String, ?> entry : vals.entrySet()) {
            setConfig(ConfigKeys.newConfigKey(Object.class, entry.getKey()), entry.getValue());
        }
    }
    
    public void removeFromLocalBag(String key) {
        // FIXME probably never worked, config key vs string ?
        ownConfig.remove(key);
    }

    protected Object coerceConfigVal(ConfigKey<?> key, Object v) {
        Object val;
        if ((v instanceof Future) || (v instanceof DeferredSupplier)) {
            // no coercion for these (coerce on exit)
            val = v;
        } else if (key instanceof StructuredConfigKey) {
            // no coercion for these structures (they decide what to do)
            val = v;
        } else if ((v instanceof Map || v instanceof Iterable) && key.getType().isInstance(v)) {
            // don't do coercion on put for these, if the key type is compatible, 
            // because that will force resolution deeply
            val = v;
        } else {
            try {
                // try to coerce on input, to detect errors sooner
                val = TypeCoercions.coerce(v, key.getTypeToken());
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
        return val;
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
    
    @Override
    public String toString() {
        Map<ConfigKey<?>, Object> sanitizeConfig;
        synchronized (ownConfig) {
            sanitizeConfig = Sanitizer.sanitize(ownConfig);
        }
        return super.toString()+"[local="+sanitizeConfig+"]";
    }

}
