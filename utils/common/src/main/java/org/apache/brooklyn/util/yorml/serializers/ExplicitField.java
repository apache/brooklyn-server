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
package org.apache.brooklyn.util.yorml.serializers;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.YormlContinuation;

import com.google.common.base.Objects;

/* On read, after InstantiateType populates the `fields` key in YamlKeysOnBlackboard,
 * look for any field(s) matching known aliases and rename them there as the fieldName,
 * so FieldsInMapUnderFields will then set it in the java object correctly.
 * <p>
 * On write, after FieldsInMapUnderFields sets the `fields` map,
 * look for the field name, and rewrite under the preferred alias at the root. */
public class ExplicitField extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }
    
    protected String fieldName;
    protected String fieldType;
    
    protected String keyName;
    public String getPreferredKeyName() { 
        if (keyName!=null) return keyName;
        // TODO mangle
        return fieldName; 
    }
    public Iterable<String> getKeyNameAndAliases() {
        // TODO make transient
        MutableSet<String> keyNameAndAliases = MutableSet.of();
        keyNameAndAliases.addIfNotNull(keyName);
        keyNameAndAliases.addIfNotNull(fieldName);
        return keyNameAndAliases; 
    }
    
    Boolean required;
    Maybe<Object> defaultValue;
    
    public class Worker extends YormlSerializerWorker {
        
        // TODO not used
        Map<String,Object> getFieldsFromYamlsKeyOnBlackboard() {
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            if (fields==null) {
                // should have been created by InstantiateType
                throw new IllegalStateException("fields should be set as a yaml key on the blackboard");
            }
            return fields;
        }
        
        public YormlContinuation read() {
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            /*
             * if fields is null either we are too early (not yet set by instantiate-type)
             * or too late (already read in to java), so we bail and this yaml key cannot be handled
             */
            if (fields==null) return YormlContinuation.CONTINUE_UNCHANGED;

            if (ExplicitFieldsBlackboard.get(blackboard).isFieldDone(fieldName)) return YormlContinuation.CONTINUE_UNCHANGED;
            ExplicitFieldsBlackboard.get(blackboard).setRequiredIfUnset(fieldName, required);
            
            int keysMatched = 0;
            boolean rerunNeeded = false;
            for (String alias: getKeyNameAndAliases()) {
                Maybe<Object> value = peekFromYamlKeysOnBlackboard(alias, Object.class);
                boolean fieldAlreadyHandled = fields.containsKey(fieldName);
                if (value.isAbsent() && !fieldAlreadyHandled) {
                    // should we take this as the default
                    if (ExplicitFieldsBlackboard.get(blackboard).shouldUseDefaultFrom(fieldName, ExplicitField.this)) {
                        // this was determined as the serializer to use for defaults on a previous pass
                        value = defaultValue==null ? Maybe.absentNoTrace("no default value") : defaultValue;;
                    }
                    if (value.isAbsent()) {
                        // use this as default on subsequent run if appropriate
                        if (!ExplicitFieldsBlackboard.get(blackboard).hasDefault(fieldName) && defaultValue!=null && defaultValue.isPresent()) {
                            ExplicitFieldsBlackboard.get(blackboard).setUseDefaultFrom(fieldName, ExplicitField.this);
                            rerunNeeded = true;
                        }
                        continue;
                    }
                }
                if (value.isPresent() && fieldAlreadyHandled) {
                    // already present
                    // TODO throw if different
                    continue;
                }
                // value present, field not yet handled
                ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                removeFromYamlKeysOnBlackboard(alias);
                fields.put(fieldName, value.get());
                keysMatched++;
            }
            return keysMatched > 0 || rerunNeeded ? YormlContinuation.CONTINUE_THEN_RERUN : YormlContinuation.CONTINUE_UNCHANGED;
        }

        public YormlContinuation write() {
            if (ExplicitFieldsBlackboard.get(blackboard).isFieldDone(fieldName)) return YormlContinuation.CONTINUE_UNCHANGED;
            
            // first pass determines what is required and what the default is 
            // (could run this on other passes so not completely efficient)
            ExplicitFieldsBlackboard.get(blackboard).setRequiredIfUnset(fieldName, required);
            if (ExplicitFieldsBlackboard.get(blackboard).getDefault(fieldName).isAbsent() && defaultValue!=null && defaultValue.isPresent()) {
                ExplicitFieldsBlackboard.get(blackboard).setUseDefaultFrom(fieldName, ExplicitField.this, defaultValue.get());
                return YormlContinuation.CONTINUE_THEN_RERUN;
            }
            
            if (!isYamlMap()) return YormlContinuation.CONTINUE_UNCHANGED;

            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            if (fields==null) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Maybe<Object> dv = ExplicitFieldsBlackboard.get(blackboard).getDefault(fieldName);
            
            if (!fields.containsKey(fieldName)) {
                // field not present, so null (or not known)
                if ((dv.isPresent() && dv.isNull()) || (!ExplicitFieldsBlackboard.get(blackboard).isRequired(fieldName) && dv.isAbsent())) {
                    // if default is null, or if not required and no default, we can suppress
                    ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                    return YormlContinuation.CONTINUE_UNCHANGED;
                }
                // default is non-null or field is required, so write the explicit null
                fields.put(getPreferredKeyName(), null);
                ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                return YormlContinuation.CONTINUE_THEN_RERUN;
            }
            
            Object value = fields.remove(fieldName);
            ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
            
            // field present
            if (dv.isPresent() && Objects.equal(dv.get(), value)) {
                // suppress if it equals the default
                return YormlContinuation.CONTINUE_UNCHANGED;
            }
            fields.put(getPreferredKeyName(), value);
            return YormlContinuation.CONTINUE_THEN_RERUN;
        }
    }
    
}
