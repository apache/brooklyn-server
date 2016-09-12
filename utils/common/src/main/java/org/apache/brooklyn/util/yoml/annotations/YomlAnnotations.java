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
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.serializers.ExplicitFieldSerializer;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;

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
    
    public Collection<ExplicitFieldSerializer> findExplicitFieldSerializers(Class<?> t, boolean requireAnnotation) {
        List<ExplicitFieldSerializer> result = MutableList.of();
        Map<String,Field> fields = YomlUtils.getAllNonTransientNonStaticFields(t, null);
        for (Map.Entry<String, Field> f: fields.entrySet()) {
            if (!requireAnnotation || f.getValue().isAnnotationPresent(YomlFieldAtTopLevel.class))
            result.add(new ExplicitFieldSerializer(f.getKey(), f.getValue()));
        }
        return result;
    }
    
    public Collection<YomlSerializer> findConfigMapSerializers(Class<?> t) {
        YomlConstructorConfigMap ann = t.getAnnotation(YomlConstructorConfigMap.class);
        if (ann==null) return Collections.emptyList();
        return new InstantiateTypeFromRegistryUsingConfigMap.Factory().newConfigKeySerializersForType(
            t,
            ann.value(), ann.writeAsKey()!=null ? ann.writeAsKey() : ann.value(),
            ann.validateAheadOfTime(), ann.requireStaticKeys());
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
        // if it takes a config map
        result.addAll(findConfigMapSerializers(type));

        // explicit fields
        YomlAllFieldsAtTopLevel allFields = type.getAnnotation(YomlAllFieldsAtTopLevel.class);
        result.addAll(findExplicitFieldSerializers(type, allFields==null));
        
        // (so far the above is the only type of serializer we pick up from annotations)
        // subclasses can extend
    }

}
