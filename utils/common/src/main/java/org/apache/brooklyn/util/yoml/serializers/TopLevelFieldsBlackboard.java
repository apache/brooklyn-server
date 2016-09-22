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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlRequirement;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.serializers.TopLevelFieldSerializer.FieldConstraint;

import com.google.common.reflect.TypeToken;

public class TopLevelFieldsBlackboard implements YomlRequirement {

    public static final String KEY = TopLevelFieldsBlackboard.class.getCanonicalName();

    public static TopLevelFieldsBlackboard get(Map<Object,Object> blackboard, String mode) {
        Object v = blackboard.get(KEY+":"+mode);
        if (v==null) {
            v = new TopLevelFieldsBlackboard();
            blackboard.put(KEY+":"+mode, v);
        }
        return (TopLevelFieldsBlackboard) v;
    }
    
    private final Map<String,String> keyNames = MutableMap.of();
    private final Map<String,Boolean> aliasesInheriteds = MutableMap.of();
    private final Map<String,Boolean> aliasesStricts = MutableMap.of();
    private final Map<String,Set<String>> aliases = MutableMap.of();
    private final Set<String> fieldsDone = MutableSet.of();
    private final Map<String,FieldConstraint> fieldsConstraints = MutableMap.of();
    private final Map<String,YomlSerializer> defaultValueForFieldComesFromSerializer = MutableMap.of();
    private final Map<String,Object> defaultValueOfField = MutableMap.of();
    private final Map<String,TypeToken<?>> declaredTypeOfFieldsAndAliases = MutableMap.of();
    
    public String getKeyName(String fieldName) {
        return Maybe.ofDisallowingNull(keyNames.get(fieldName)).orNull();
    }
    public void setKeyNameIfUnset(String fieldName, String keyName) {
        if (keyName==null) return;
        if (keyNames.get(fieldName)!=null) return;
        keyNames.put(fieldName, keyName);
    }
    public void setAliasesInheritedIfUnset(String fieldName, Boolean aliasesInherited) {
        if (aliasesInherited==null) return;
        if (aliasesInheriteds.get(fieldName)!=null) return;
        aliasesInheriteds.put(fieldName, aliasesInherited);
    }
    public boolean isAliasesStrict(String fieldName) {
        return Boolean.TRUE.equals(aliasesStricts.get(fieldName));
    }
    public void setAliasesStrictIfUnset(String fieldName, Boolean aliasesStrict) {
        if (aliasesStrict==null) return;
        if (aliasesStricts.get(fieldName)!=null) return;
        aliasesStricts.put(fieldName, aliasesStrict);
    }
    public void addAliasIfNotDisinherited(String fieldName, String alias) {
        addAliasesIfNotDisinherited(fieldName, MutableList.<String>of().appendIfNotNull(alias));
    }
    public void addAliasesIfNotDisinherited(String fieldName, List<String> aliases) {
        if (Boolean.FALSE.equals(aliasesInheriteds.get(fieldName))) {
            // no longer heritable
            return;
        }
        Set<String> aa = this.aliases.get(fieldName);
        if (aa==null) {
            aa = MutableSet.of();
            this.aliases.put(fieldName, aa);
        }
        if (aliases==null) return;
        for (String alias: aliases) { if (Strings.isNonBlank(alias)) { aa.add(alias); } }
    }
    public Collection<? extends String> getAliases(String fieldName) {
        Set<String> aa = this.aliases.get(fieldName);
        if (aa==null) return MutableSet.of();
        return aa;
    }

    public Maybe<FieldConstraint> getConstraint(String fieldName) {
        return Maybe.ofDisallowingNull(fieldsConstraints.get(fieldName));
    }
    public void setConstraintIfUnset(String fieldName, FieldConstraint constraint) {
        if (constraint==null) return;
        if (fieldsConstraints.get(fieldName)!=null) return;
        fieldsConstraints.put(fieldName, constraint);
    }
    @Override
    public void checkCompletion(YomlContext context) {
        List<String> incompleteRequiredFields = MutableList.of();
        for (Map.Entry<String,FieldConstraint> fieldConstraint: fieldsConstraints.entrySet()) {
            FieldConstraint v = fieldConstraint.getValue();
            if (v!=null && FieldConstraint.REQUIRED==v && !fieldsDone.contains(fieldConstraint.getKey())) {
                incompleteRequiredFields.add(fieldConstraint.getKey());
            }
        }
        if (!incompleteRequiredFields.isEmpty()) {
            throw new YomlException("Missing one or more explicitly required fields: "+Strings.join(incompleteRequiredFields, ", "), context);
        }
    }

    public boolean isFieldDone(String fieldName) {
        return fieldsDone.contains(fieldName);
    }
    public void setFieldDone(String fieldName) {
        fieldsDone.add(fieldName);
    }

    public void setUseDefaultFrom(String fieldName, YomlSerializer topLevelField, Object defaultValue) {
        defaultValueForFieldComesFromSerializer.put(fieldName, topLevelField);
        defaultValueOfField.put(fieldName, defaultValue);
    }
    public boolean shouldUseDefaultFrom(String fieldName, YomlSerializer topLevelField) {
        return topLevelField.equals(defaultValueForFieldComesFromSerializer.get(fieldName));
    }
    public Maybe<Object> getDefault(String fieldName) {
        if (!defaultValueOfField.containsKey(fieldName)) return Maybe.absent("no default");
        return Maybe.of(defaultValueOfField.get(fieldName));
    }
    
    /** optional, and must be called after aliases; not used for fields, is used for config keys */
    public void setDeclaredTypeIfUnset(String fieldName, TypeToken<?> type) {
        setDeclaredTypeOfIndividualNameOrAliasIfUnset(fieldName, type);
        for (String alias: getAliases(fieldName)) 
            setDeclaredTypeOfIndividualNameOrAliasIfUnset(alias, type);
    }
    protected void setDeclaredTypeOfIndividualNameOrAliasIfUnset(String fieldName, TypeToken<?> type) {
        if (declaredTypeOfFieldsAndAliases.get(fieldName)!=null) return;
        declaredTypeOfFieldsAndAliases.put(fieldName, type);
    }
    /** only if {@link #setDeclaredTypeIfUnset(String, TypeToken)} is being used (eg config keys) */
    public TypeToken<?> getDeclaredType(String key) {
        return declaredTypeOfFieldsAndAliases.get(key);
    }
    
}
