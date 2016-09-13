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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.YomlContext.StandardPhases;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/* On read, after InstantiateType populates the `fields` key in YamlKeysOnBlackboard,
 * look for any field(s) matching known aliases and rename them there as the fieldName,
 * so FieldsInMapUnderFields will then set it in the java object correctly.
 * <p>
 * On write, after FieldsInMapUnderFields sets the `fields` map,
 * look for the field name, and rewrite under the preferred alias at the root. */
@YomlAllFieldsTopLevel
@Alias("top-level-field")
public class TopLevelFieldSerializer extends YomlSerializerComposition {

    private static final Logger log = LoggerFactory.getLogger(TopLevelFieldSerializer.class);
    
    public TopLevelFieldSerializer() {}
    public TopLevelFieldSerializer(Field f) {
        this(f.getName(), f);
    }
    /** preferred constructor for dealing with shadowed fields using superclass.field naming convention */
    public TopLevelFieldSerializer(String name, Field f) {
        fieldName = keyName = name;
            
        Alias alias = f.getAnnotation(Alias.class);
        if (alias!=null) {
            aliases = MutableList.of();
            if (Strings.isNonBlank(alias.preferred())) {
                keyName = alias.preferred();
                aliases.add(alias.preferred());
            }
            aliases.add(f.getName());
            aliases.addAll(Arrays.asList(alias.value()));
        }
        
        // if there are other things on ytf
//        YomlFieldAtTopLevel ytf = f.getAnnotation(YomlFieldAtTopLevel.class);
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    /** field in java class to read/write */ 
    protected String fieldName;
    
    // not used at present, but would simplify expressing default values
    // TODO we could also conceivably infer the expected field type better
//    protected String fieldType;
    
    /** key to write at root in yaml */
    protected String keyName;
    /** convenience if supplying a single item in {@link #aliases} */
    protected String alias;
    /** aliases to recognise at root in yaml when reading, in addition to {@link #keyName} and normally {@link #fieldName} */
    protected List<String> aliases;
    
    /** by default when multiple top-level-field serializers are supplied for the same {@link #fieldName}, all aliases are accepted;
     * set this false to restrict to those in the first such serializer */
    protected Boolean aliasesInherited;
    /** by default aliases are taken case-insensitive, with mangling supported,
     * and including the {@link #fieldName} as an alias;
     * set false to disallow all these, recognising only the explicitly noted 
     * {@link #keyName} and {@link #aliases} as keys (but still defaulting to {@link #fieldName} if {@link #keyName} is absent) */
    protected Boolean aliasesStrict;
    
    public static enum FieldConstraint { REQUIRED } 
    /** by default fields can be left null; set {@link FieldConstraint#REQUIRED} to require a value to be supplied (or a default set);
     * other constraints may be introduded, and API may change, but keyword `required` will be coercible to this */
    protected FieldConstraint constraint;
    
    /** a default value to use when reading (and to use to determine whether to omit the field when writing) */
    // TODO would be nice to support maybe here, not hard here, but it makes it hard to set from yaml
    // also keyword `default` as alias
    protected Object defaultValue;
    
    protected String getKeyNameForMapOfGeneralValues() {
        return FieldsInMapUnderFields.KEY_NAME_FOR_MAP_OF_FIELD_VALUES;
    }
    
    public class Worker extends YomlSerializerWorker {
        
        final static String PREPARING_TOP_LEVEL_FIELDS = "preparing-top-level-fields";
        
        protected String getPreferredKeyName() { 
            String result = getTopLevelFieldsBlackboard().getKeyName(fieldName);
            if (result!=null) return result;
            return fieldName; 
        }

        protected TopLevelFieldsBlackboard getTopLevelFieldsBlackboard() {
            return TopLevelFieldsBlackboard.get(blackboard, getKeyNameForMapOfGeneralValues());
        }
        
        protected Iterable<String> getKeyNameAndAliases() {
            MutableSet<String> keyNameAndAliases = MutableSet.of();
            keyNameAndAliases.addIfNotNull(getPreferredKeyName());
            if (!getTopLevelFieldsBlackboard().isAliasesStrict(fieldName)) {
                keyNameAndAliases.addIfNotNull(fieldName);
            }
            keyNameAndAliases.addAll(getTopLevelFieldsBlackboard().getAliases(fieldName));
            return keyNameAndAliases; 
        }

        protected boolean readyForMainEvent() {
            if (!context.seenPhase(YomlContext.StandardPhases.HANDLING_TYPE)) return false;
            if (context.willDoPhase(YomlContext.StandardPhases.HANDLING_TYPE)) return false;
            if (!context.seenPhase(PREPARING_TOP_LEVEL_FIELDS)) {
                if (context.isPhase(YomlContext.StandardPhases.MANIPULATING)) {
                    // interrupt the manipulating phase to do a preparing phase
                    context.phaseInsert(PREPARING_TOP_LEVEL_FIELDS, StandardPhases.MANIPULATING);
                    context.phaseAdvance();
                    return false;
                }
            }
            if (context.isPhase(PREPARING_TOP_LEVEL_FIELDS)) {
                prepareTopLevelFields();
                return false;
            }
            if (getTopLevelFieldsBlackboard().isFieldDone(fieldName)) return false;
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return false;
            return true;
        }

        protected void prepareTopLevelFields() {
            // do the pre-main pass to determine what is required for top-level fields and what the default is 
            getTopLevelFieldsBlackboard().setKeyNameIfUnset(fieldName, keyName);
            getTopLevelFieldsBlackboard().addAliasIfNotDisinherited(fieldName, alias);
            getTopLevelFieldsBlackboard().addAliasesIfNotDisinherited(fieldName, aliases);
            getTopLevelFieldsBlackboard().setAliasesInheritedIfUnset(fieldName, aliasesInherited);
            getTopLevelFieldsBlackboard().setAliasesStrictIfUnset(fieldName, aliasesStrict);
            getTopLevelFieldsBlackboard().setConstraintIfUnset(fieldName, constraint);
            if (getTopLevelFieldsBlackboard().getDefault(fieldName).isAbsent() && defaultValue!=null) {
                getTopLevelFieldsBlackboard().setUseDefaultFrom(fieldName, TopLevelFieldSerializer.this, defaultValue);
            }
            // TODO combine aliases, other items
        }
        
        protected boolean canDoRead() { return hasJavaObject(); }
        
        public void read() {
            if (!readyForMainEvent()) return;
            if (!canDoRead()) return;
            if (!isYamlMap()) return;
            if (!hasYamlKeysOnBlackboard()) return;
            
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard(getKeyNameForMapOfGeneralValues(), Map.class).orNull();
            if (fields==null) {
                // create the fields if needed; FieldsInFieldsMap will remove (even if empty)
                fields = MutableMap.of();
                YamlKeysOnBlackboard.getOrCreate(blackboard, null).yamlKeysToReadToJava.put(getKeyNameForMapOfGeneralValues(), fields);
            }
            
            int keysMatched = 0;
            for (String aliasO: getKeyNameAndAliases()) {
                Set<String> aliasMangles = getTopLevelFieldsBlackboard().isAliasesStrict(fieldName) ?
                    Collections.singleton(aliasO) : findAllKeyManglesYamlKeys(aliasO);
                for (String alias: aliasMangles) {
                    Maybe<Object> value = peekFromYamlKeysOnBlackboard(alias, Object.class);
                    if (value.isAbsent()) continue;
                    if (log.isTraceEnabled()) {
                        log.trace(TopLevelFieldSerializer.this+": found "+alias+" for "+fieldName);
                    }
                    boolean fieldAlreadyKnown = fields.containsKey(fieldName);
                    if (value.isPresent() && fieldAlreadyKnown) {
                        // already present
                        if (!Objects.equal(value.get(), fields.get(fieldName))) {
                            throw new IllegalStateException("Cannot set '"+fieldName+"' to '"+value.get()+"' supplied in '"+alias+"' because this conflicts with '"+fields.get(fieldName)+"' already set");
                        }
                        continue;
                    }
                    // value present, field not yet handled
                    removeFromYamlKeysOnBlackboard(alias);
                    fields.put(fieldName, value.get());
                    keysMatched++;
                }
            }
            if (keysMatched==0) {
                // set a default if there is one
                Maybe<Object> value = getTopLevelFieldsBlackboard().getDefault(fieldName);
                if (value.isPresentAndNonNull()) {
                    fields.put(fieldName, value.get());
                    keysMatched++;                    
                }
            }
            if (keysMatched>0) {
                // repeat the preparing phase if we set any keys, so that remapping can apply
                getTopLevelFieldsBlackboard().setFieldDone(fieldName);
                context.phaseInsert(StandardPhases.MANIPULATING);
            }
        }

        public void write() {
            if (!readyForMainEvent()) return;
            if (!isYamlMap()) return;

            @SuppressWarnings("unchecked")
            Map<String,Object> fields = getFromYamlMap(getKeyNameForMapOfGeneralValues(), Map.class).orNull();
            /*
             * if fields is null either we are too early (not yet set by instantiate-type / FieldsInMapUnderFields)
             * or too late (already read in to java), so we bail -- this yaml key cannot be handled at this time
             */
            if (fields==null) return;
            
            Maybe<Object> dv = getTopLevelFieldsBlackboard().getDefault(fieldName);
            Maybe<Object> valueToSet;
            
            if (!fields.containsKey(fieldName)) {
                // field not present, so omit (if field is not required and no default, or if default value is present and null) 
                // else write an explicit null
                if ((dv.isPresent() && dv.isNull()) || (getTopLevelFieldsBlackboard().getConstraint(fieldName).orNull()!=FieldConstraint.REQUIRED && dv.isAbsent())) {
                    // if default is null, or if not required and no default, we can suppress
                    getTopLevelFieldsBlackboard().setFieldDone(fieldName);
                    return;
                }
                // default is non-null or field is required, so write the explicit null
                valueToSet = Maybe.ofAllowingNull(null);
            } else {
                // field present
                valueToSet = Maybe.of(fields.remove(fieldName));
                if (dv.isPresent() && Objects.equal(dv.get(), valueToSet.get())) {
                    // suppress if it equals the default
                    getTopLevelFieldsBlackboard().setFieldDone(fieldName);
                    valueToSet = Maybe.absent();
                }
            }
            
            if (valueToSet.isPresent()) {
                getTopLevelFieldsBlackboard().setFieldDone(fieldName);
                Object oldValue = getYamlMap().put(getPreferredKeyName(), valueToSet.get());
                if (oldValue!=null && !oldValue.equals(valueToSet.get())) {
                    throw new IllegalStateException("Conflicting values for `"+getPreferredKeyName()+"`");
                }
                // and move the `fields` object to the end
                getYamlMap().remove(getKeyNameForMapOfGeneralValues());
                if (!fields.isEmpty())
                    getYamlMap().put(getKeyNameForMapOfGeneralValues(), fields);
                // rerun this phase again, as we've changed it
                context.phaseInsert(StandardPhases.MANIPULATING);
            }
        }
    }

    @Override
    public String toString() {
        return "top-level-field["+fieldName+"->"+keyName+":"+alias+"/"+aliases+"]";
    }
}
