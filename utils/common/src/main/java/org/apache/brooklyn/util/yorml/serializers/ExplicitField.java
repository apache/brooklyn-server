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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlContext.StandardPhases;

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
    protected String alias;
    protected List<String> aliases;
    
    public String getPreferredKeyName() { 
        if (keyName!=null) return keyName;
        // TODO mangle
        return fieldName; 
    }
    public Iterable<String> getKeyNameAndAliases() {
        // TODO use transient cache
        MutableSet<String> keyNameAndAliases = MutableSet.of();
        keyNameAndAliases.addIfNotNull(keyName);
        keyNameAndAliases.addIfNotNull(fieldName);
        keyNameAndAliases.addIfNotNull(alias);
        keyNameAndAliases.putAll(aliases);
        return keyNameAndAliases; 
    }
    
    Boolean required;
    // TODO would be nice to support maybe here, only one reference, but it makes it hard to set
    Object defaultValue;
    
    public class Worker extends YormlSerializerWorker {
        
        String PREPARING_EXPLICIT_FIELDS = "preparing-explicit-fields";
        
        protected boolean readyForMainEvent() {
            if (!context.seenPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return false;
            if (!context.seenPhase(PREPARING_EXPLICIT_FIELDS)) {
                if (context.isPhase(YormlContext.StandardPhases.MANIPULATING)) {
                    // interrupt the manipulating phase to do a preparing phase
                    context.phaseInsert(PREPARING_EXPLICIT_FIELDS, StandardPhases.MANIPULATING);
                    context.phaseAdvance();
                    return false;
                }
            }
            if (context.isPhase(PREPARING_EXPLICIT_FIELDS)) {
                // do the pre-main pass to determine what is required for explicit fields and what the default is 
                ExplicitFieldsBlackboard.get(blackboard).setRequiredIfUnset(fieldName, required);
                if (ExplicitFieldsBlackboard.get(blackboard).getDefault(fieldName).isAbsent() && defaultValue!=null) {
                    ExplicitFieldsBlackboard.get(blackboard).setUseDefaultFrom(fieldName, ExplicitField.this, defaultValue);
                }
                // TODO combine aliases, other items
                return false;
            }
            if (ExplicitFieldsBlackboard.get(blackboard).isFieldDone(fieldName)) return false;
            if (!context.isPhase(YormlContext.StandardPhases.MANIPULATING)) return false;
            return true;
        }
        
        public void read() {
            if (!readyForMainEvent()) return; 
            if (!hasJavaObject()) return;
            if (!isYamlMap()) return;
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            
            /*
             * if fields is null either we are too early (not yet set by instantiate-type)
             * or too late (already read in to java), so we bail -- this yaml key cannot be handled at this time
             */
            if (fields==null) return;

            int keysMatched = 0;
            for (String alias: getKeyNameAndAliases()) {
                Maybe<Object> value = peekFromYamlKeysOnBlackboard(alias, Object.class);
                if (value.isAbsent()) continue;
                boolean fieldAlreadyKnown = fields.containsKey(fieldName);
                if (value.isPresent() && fieldAlreadyKnown) {
                    // already present
                    // TODO throw if different
                    continue;
                }
                // value present, field not yet handled
                removeFromYamlKeysOnBlackboard(alias);
                fields.put(fieldName, value.get());
                keysMatched++;
            }
            if (keysMatched==0) {
                // set a default if there is one
                Maybe<Object> value = ExplicitFieldsBlackboard.get(blackboard).getDefault(fieldName);
                if (value.isPresentAndNonNull()) {
                    fields.put(fieldName, value.get());
                    keysMatched++;                    
                }
            }
            if (keysMatched>0) {
                // repeat the preparing phase if we set any keys, so that remapping can apply
                ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                context.phaseInsert(StandardPhases.MANIPULATING);
            }
        }

        public void write() {
            if (!readyForMainEvent()) return;
            if (!isYamlMap()) return;

            @SuppressWarnings("unchecked")
            Map<String,Object> fields = getFromYamlMap("fields", Map.class).orNull();
            if (fields==null) return;
            
            Maybe<Object> dv = ExplicitFieldsBlackboard.get(blackboard).getDefault(fieldName);
            Maybe<Object> valueToSet;
            
            if (!fields.containsKey(fieldName)) {
                // field not present, so null (or not known)
                if ((dv.isPresent() && dv.isNull()) || (!ExplicitFieldsBlackboard.get(blackboard).isRequired(fieldName) && dv.isAbsent())) {
                    // if default is null, or if not required and no default, we can suppress
                    ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                    return;
                }
                // default is non-null or field is required, so write the explicit null
                valueToSet = Maybe.ofAllowingNull(null);
            } else {
                // field present
                valueToSet = Maybe.of(fields.remove(fieldName));
                if (dv.isPresent() && Objects.equal(dv.get(), valueToSet.get())) {
                    // suppress if it equals the default
                    valueToSet = Maybe.absent();
                }
            }
            
            if (valueToSet.isPresent()) {
                ExplicitFieldsBlackboard.get(blackboard).setFieldDone(fieldName);
                Object oldValue = getYamlMap().put(getPreferredKeyName(), valueToSet.get());
                if (oldValue!=null && !oldValue.equals(valueToSet.get())) {
                    throw new IllegalStateException("Conflicting values for `"+getPreferredKeyName()+"`");
                }
                // and move the `fields` object to the end
                getYamlMap().remove("fields");
                getYamlMap().put("fields", fields);
                // rerun this phase again, as we've changed it
                context.phaseInsert(StandardPhases.MANIPULATING);
            }
        }
    }
    
}
