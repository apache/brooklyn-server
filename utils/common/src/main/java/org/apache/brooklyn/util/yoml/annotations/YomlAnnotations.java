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
package org.apache.brooklyn.util.yoml.annotations;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultKey;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultValue;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.serializers.ConvertFromPrimitive;
import org.apache.brooklyn.util.yoml.serializers.ConvertSingletonMap;
import org.apache.brooklyn.util.yoml.serializers.DefaultMapValuesSerializer;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;
import org.apache.brooklyn.util.yoml.serializers.RenameKeySerializer;
import org.apache.brooklyn.util.yoml.serializers.RenameKeySerializer.RenameDefaultKey;
import org.apache.brooklyn.util.yoml.serializers.RenameKeySerializer.RenameDefaultValue;
import org.apache.brooklyn.util.yoml.serializers.TopLevelFieldSerializer;
import org.apache.brooklyn.util.yoml.serializers.TypeFromOtherFieldSerializer;

public class YomlAnnotations {

    public Set<String> findTypeNamesFromAnnotations(Class<?> type, String optionalDefaultPreferredTypeName, boolean includeJavaTypeNameEvenIfOthers) {
        MutableSet<String> names = MutableSet.of();
        
        Alias overallAlias = type.getAnnotation(Alias.class);
        if (optionalDefaultPreferredTypeName!=null) {
            names.addIfNotNull(optionalDefaultPreferredTypeName);
        }
        if (overallAlias!=null) {
            if (Strings.isNonBlank(overallAlias.preferred())) {
                names.add( overallAlias.preferred() );
            }
            names.addAll( Arrays.asList(overallAlias.value()) );
        }
        if (includeJavaTypeNameEvenIfOthers || names.isEmpty()) {
            names.add(type.getName());
        }

        return names;
    }
    
    public Collection<YomlSerializer> findTopLevelFieldSerializers(Class<?> t, boolean requireAnnotation) {
        List<YomlSerializer> result = MutableList.of();
        Map<String,Field> fields = YomlUtils.getAllNonTransientNonStaticFields(t, null);
        for (Map.Entry<String, Field> f: fields.entrySet()) {
            if (!requireAnnotation || f.getValue().isAnnotationPresent(YomlTopLevelField.class)) {
                result.add(new TopLevelFieldSerializer(f.getKey(), f.getValue()));
                YomlTypeFromOtherField typeFromOther = f.getValue().getAnnotation(YomlTypeFromOtherField.class);
                if (typeFromOther!=null) {
                    result.add(new TypeFromOtherFieldSerializer(f.getKey(), typeFromOther));
                }
            }
        }
        return result;
    }
    
    public Collection<YomlSerializer> findConfigMapConstructorSerializersIgnoringConfigInheritance(Class<?> t) {
        YomlConfigMapConstructor ann = t.getAnnotation(YomlConfigMapConstructor.class);
        if (ann==null) return Collections.emptyList();
        return InstantiateTypeFromRegistryUsingConfigMap.newFactoryIgnoringInheritance().newConfigKeySerializersForType(
            t,
            ann.value(), Strings.isNonBlank(ann.writeAsKey()) ? ann.writeAsKey() : ann.value(),
            ann.validateAheadOfTime(), ann.requireStaticKeys());
    }

    public Collection<YomlSerializer> findSingletonMapSerializers(Class<?> t) {
        YomlSingletonMap ann = t.getAnnotation(YomlSingletonMap.class);
        if (ann==null) return Collections.emptyList();
        return MutableList.of((YomlSerializer) new ConvertSingletonMap(ann));
    }

    public Collection<YomlSerializer> findConvertFromPrimitiveSerializers(Class<?> t) {
        YomlFromPrimitive ann = t.getAnnotation(YomlFromPrimitive.class);
        if (ann==null) return Collections.emptyList();
        return MutableList.of((YomlSerializer) new ConvertFromPrimitive(ann));
    }

    public Collection<YomlSerializer> findDefaultMapValuesSerializers(Class<?> t) {
        YomlDefaultMapValues ann = t.getAnnotation(YomlDefaultMapValues.class);
        if (ann==null) return Collections.emptyList();
        return MutableList.of((YomlSerializer) new DefaultMapValuesSerializer(ann));
    }

    public Collection<YomlSerializer> findRenameKeySerializers(Class<?> t) {
        MutableList<YomlSerializer> result = MutableList.of();
        YomlRenameKey ann1 = t.getAnnotation(YomlRenameKey.class);
        if (ann1!=null) result.add(new RenameKeySerializer(ann1));
        YomlRenameDefaultKey ann2 = t.getAnnotation(YomlRenameDefaultKey.class);
        if (ann2!=null) result.add(new RenameDefaultKey(ann2));
        YomlRenameDefaultValue ann3 = t.getAnnotation(YomlRenameDefaultValue.class);
        if (ann3!=null) result.add(new RenameDefaultValue(ann3));
        return result;
    }

    /** Adds the default set of serializer annotations */
    public Set<YomlSerializer> findSerializerAnnotations(Class<?> type, boolean recurseUpIfEmpty) {
        Set<YomlSerializer> result = MutableSet.of();
        if (type==null) return result;
        
        collectSerializerAnnotationsAtClass(result, type);
        boolean canRecurse = result.isEmpty();
        
        if (recurseUpIfEmpty && canRecurse) {
            result.addAll(findSerializerAnnotations(type.getSuperclass(), recurseUpIfEmpty));
        }
        return result;
    }
    
    protected void collectSerializerAnnotationsAtClass(Set<YomlSerializer> result, Class<?> type) {
        collectSerializersLowLevel(result, type);
        collectSerializersForConfig(result, type);
        collectSerializersFields(result, type);
        // subclasses can extend or override the methods above
    }

    protected void collectSerializersFields(Set<YomlSerializer> result, Class<?> type) {
        YomlAllFieldsTopLevel allFields = type.getAnnotation(YomlAllFieldsTopLevel.class);
        result.addAll(findTopLevelFieldSerializers(type, allFields==null));
    }

    protected void collectSerializersForConfig(Set<YomlSerializer> result, Class<?> type) {
        result.addAll(findConfigMapConstructorSerializersIgnoringConfigInheritance(type));
    }

    protected void collectSerializersLowLevel(Set<YomlSerializer> result, Class<?> type) {
        result.addAll(findConvertFromPrimitiveSerializers(type));
        result.addAll(findRenameKeySerializers(type));
        result.addAll(findSingletonMapSerializers(type));
        result.addAll(findDefaultMapValuesSerializers(type));
    }
    
}
