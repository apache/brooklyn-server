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
package org.apache.brooklyn.util.yoml.serializers;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yoml.internal.YomlContext;

import com.google.common.base.Preconditions;

@Alias("config-map-constructor")
/** Special instantiator for when the class's constructor takes a Map<String,Object> of config */
public class InstantiateTypeFromRegistryUsingConfigMap extends InstantiateTypeFromRegistry {

    public static final String PHASE_INSTANTIATE_TYPE_DEFERRED = "handling-type-deferred-after-config";

    protected String keyNameForConfigWhenSerialized = null;
    protected String fieldNameForConfigInJava = null;
    boolean staticKeysRequired;
    
    // don't currently fully support inferring setup from annotations; we need the field above.
    // easily could automate with a YomlConfigMap annotation - but for now make it explicit
    // (for now this field can be used to load explicit config keys, if the field name is supplied)
    boolean inferByScanning = false;
    
    public static Factory newFactoryIgnoringInheritance() {
        return new Factory();
    }
    
    public static class Factory {
        
        protected Factory() {}
        
        /** creates a set of serializers handling config for any type, with the given field/key combination;
         * the given field will be checked at serialization time to determine whether this is applicable */
        public Set<YomlSerializer> newConfigKeyClassScanningSerializers(
            String fieldNameForConfigInJava, String keyNameForConfigWhenSerialized, boolean requireStaticKeys) {
            
            return findSerializers(null,
                fieldNameForConfigInJava, keyNameForConfigWhenSerialized, 
                false, requireStaticKeys);
        }
        
        /** creates a set of serializers handling config for the given type, for use in a type-specific serialization,
         * permitting multiple field/key combos; if the given field is not found, the pair is excluded here */
        public Set<YomlSerializer> newConfigKeySerializersForType( 
                Class<?> type, 
                String fieldNameForConfigInJava, String keyNameForConfigWhenSerialized,
                boolean validateAheadOfTime, boolean requireStaticKeys) {
            return findSerializers(type, fieldNameForConfigInJava, keyNameForConfigWhenSerialized, validateAheadOfTime, requireStaticKeys);
        }
        
        protected Set<YomlSerializer> findSerializers( 
                Class<?> type, 
                String fieldNameForConfigInJava, String keyNameForConfigWhenSerialized,
                boolean validateAheadOfTime, boolean requireStaticKeys) {
            MutableSet<YomlSerializer> result = MutableSet.<YomlSerializer>of();
            if (fieldNameForConfigInJava==null) return result;
            InstantiateTypeFromRegistryUsingConfigMap instantiator = newInstance();
            instantiator.fieldNameForConfigInJava = fieldNameForConfigInJava;
            if (validateAheadOfTime) {
                Preconditions.checkArgument(instantiator.isValidConfigFieldOrBlankSoWeCanRead(type), "Missing config field "+fieldNameForConfigInJava+" in "+type);
                instantiator.findConstructorMaybe(type).get();
            }
            instantiator.keyNameForConfigWhenSerialized = keyNameForConfigWhenSerialized;
            instantiator.staticKeysRequired = false;

            if (type!=null) {
                instantiator.inferByScanning = false;
                result.addAll(TopLevelConfigKeySerializer.findConfigKeySerializers(keyNameForConfigWhenSerialized, type));
            } else {
                instantiator.inferByScanning = true;
            }

            result.add(new ConfigInMapUnderConfigSerializer(keyNameForConfigWhenSerialized));
            result.add(instantiator);

            return result;
        }

        protected InstantiateTypeFromRegistryUsingConfigMap newInstance() {
            return new InstantiateTypeFromRegistryUsingConfigMap();
        }
    }
    
    protected InstantiateTypeFromRegistryUsingConfigMap() {}
    
    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    class Worker extends InstantiateTypeFromRegistry.Worker {

        @Override
        public void read() {
            if (context.isPhase(PHASE_INSTANTIATE_TYPE_DEFERRED)) {
                readFinallyCreate();
            } else {
                super.read();
            }
        }

        @Override
        protected boolean readType(String type) {
            Class<?> clazz = config.getTypeRegistry().getJavaTypeMaybe(type).orNull();
            if (!isConfigurable(clazz)) return false;
            
            // prepare blackboard, annotations, then do handling_config
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.create(blackboard, keyNameForConfigWhenSerialized);
            
            fib.typeNameFromReadToConstructJavaLater = type;
            fib.typeFromReadToConstructJavaLater = clazz;
            fib.fieldsFromReadToConstructJava = MutableMap.of();
            
            addSerializersForDiscoveredRealType(type);
            addExtraTypeSerializers(clazz);
            
            context.phaseInsert(YomlContext.StandardPhases.MANIPULATING, PHASE_INSTANTIATE_TYPE_DEFERRED);
            context.phaseAdvance();
            return true;
        }

        protected void addExtraTypeSerializers(Class<?> clazz) {
            if (!inferByScanning) return;
            
            // prevent multiple additions
            if (!putLabelOnBlackboard("extra-type-serializers="+clazz, true)) return;
            
            SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(
                TopLevelConfigKeySerializer.findConfigKeySerializers(keyNameForConfigWhenSerialized, clazz) );
        }

        protected TopLevelFieldsBlackboard getTopLevelFieldsBlackboard() {
            // keys recorded here by the individual serializers
            return TopLevelFieldsBlackboard.get(blackboard, keyNameForConfigWhenSerialized);
        }

        protected void readFinallyCreate() {
            if (hasJavaObject()) return;
            
            // this is running in a later phase, after the brooklyn.config map has been set up
            // instantiate with special constructor
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard, keyNameForConfigWhenSerialized);
            Class<?> type = fib.typeFromReadToConstructJavaLater;
            if (type==null) return;
            
            Preconditions.checkNotNull(keyNameForConfigWhenSerialized);

            YomlConfig newConfig = YomlConfig.Builder.builder(config).constructionInstruction(
                newConstructor(type, getTopLevelFieldsBlackboard().getConfigKeys(), MutableMap.copyOf(fib.fieldsFromReadToConstructJava), 
                    config.getConstructionInstruction())).build();
            
            Maybe<Object> resultM = config.getTypeRegistry().newInstanceMaybe(fib.typeNameFromReadToConstructJavaLater, Yoml.newInstance(newConfig));
          
            if (resultM.isAbsent()) {
                warn(new IllegalStateException("Unable to create type '"+type+"'", ((Maybe.Absent<?>)resultM).getException()));
                return;
            }

            fib.fieldsFromReadToConstructJava.clear();
            storeReadObjectAndAdvance(resultM.get(), true);
        }
        
        @Override
        protected boolean canDoWrite() {
            if (!super.canDoWrite()) return false;
            if (!isConfigurable(getJavaObject().getClass())) return false;
            if (!isValidConfigFieldSoWeCanWrite()) return false;
            
            return true;
        }

        @Override
        protected void writingPopulateBlackboard() {
            super.writingPopulateBlackboard();

            try {
                String configMapKeyName = fieldNameForConfigInJava;
                if (configMapKeyName==null) {
                    if (!inferByScanning) {
                        throw new IllegalStateException("no config key name set and not allowed to infer; "
                            + "this serializer should only be used when the config key name is specified");
                    } else {
                        // optionally: we could support annotation on the type to learn the key name;
                        // but without that we just write as fields
                        throw new UnsupportedOperationException("config key name must be set explicitly");
                    }
                }
                // write clues for ConfigInMapUnder...
                
                JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
                Field f = Reflections.findFieldMaybe(getJavaObject().getClass(), fieldNameForConfigInJava).get();
                f.setAccessible(true);
                Map<String, Object> configMap = getRawConfigMap(f, getJavaObject());
                if (configMap!=null) {
                    fib.configToWriteFromJava = MutableMap.copyOf(configMap);
                }

                // suppress wherever the config is stored
                fib.fieldsToWriteFromJava.remove(configMapKeyName);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                warn(new IllegalStateException("Unable to retieve config map in "+getJavaObject(), e));
                return;
            }
            
            addExtraTypeSerializers(getJavaObject().getClass());
        }
        
        /** see {@link #isValidConfigFieldOrBlankSoWeCanRead(Class)}; cannot write via this if field blank
         * as we cannot reverse engineer config keys from fields */
        protected boolean isValidConfigFieldSoWeCanWrite() {
            if (Strings.isBlank(fieldNameForConfigInJava)) return false;
            return findFieldMaybe(getJavaObject().getClass()).isPresent();
        }

        protected void writingInsertPhases() {
            super.writingInsertPhases();
            // for configs, we need to do this to get type info (and preferred aliases)
            context.phaseInsert(TopLevelFieldSerializer.Worker.PREPARING_TOP_LEVEL_FIELDS);
        }

    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getRawConfigMap(Field f, Object obj) throws IllegalAccessException {
        return (Map<String,Object>)f.get(obj);
    }

    /** configurable if it has a map constructor and at least one public static config key */
    protected boolean isConfigurable(Class<?> type) {
        if (type==null) return false;
        if (findConstructorMaybe(type).isAbsent()) return false;
        if (!isValidConfigFieldOrBlankSoWeCanRead(type)) return false;
        if (staticKeysRequired && TopLevelConfigKeySerializer.findConfigKeys(type).isEmpty()) return false;
        return true;
    }

    /** can be blank if reads are supported to a constructor but no config map field is used within
     * the class, ie fields for each config value are populated on construction */
    protected boolean isValidConfigFieldOrBlankSoWeCanRead(Class<?> type) {
        if ("".equals(fieldNameForConfigInJava)) return true;
        return findFieldMaybe(type).isPresent();
    }

    protected Maybe<Field> findFieldMaybe(Class<?> type) {
        Maybe<Field> f = Reflections.findFieldMaybe(type, fieldNameForConfigInJava);
        if (f.isPresent() && !Map.class.isAssignableFrom(f.get().getType())) f = Maybe.absent();
        return f;
    }

    protected Maybe<?> findConstructorMaybe(Class<?> type) {
        return Reflections.findConstructorExactMaybe(type, Map.class);
    }

    /** 
     * creates an instruction for working with a single-argument constructor which takes a simple map of
     * config values.
     * <p>
     * this ignores inheritance since within this project specific ConfigInheritanceContext 
     * and {@link ConfigInheritance} strategies are not available.
     * <p>
     * callers will have already invoked {@link #findConstructorMaybe(Class)} so implementations can 
     * assume the constructor exists.  subclassers should ensure that {@link #findConstructorMaybe(Class)} is
     * also updated if required.  
     */
    protected ConstructionInstruction newConstructor(Class<?> type, Map<String, ConfigKey<?>> keysByAlias,
            Map<String, Object> fieldsFromReadToConstructJava, ConstructionInstruction optionalOuter) {
        return ConstructionInstructions.Factory.newUsingConstructorWithArgs(type, 
            MutableList.of(fieldsFromReadToConstructJava), optionalOuter);
    }

}
