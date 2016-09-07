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
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.serializers.ExplicitField;

public class YomlAnnotations {

    public static Set<String> findTypeNamesFromAnnotations(Class<?> type, String optionalDefaultPreferredName, boolean includeJavaTypeNameEvenIfOthers) {
        MutableSet<String> names = MutableSet.of();
        
        Alias overallAlias = type.getAnnotation(Alias.class);
        if (optionalDefaultPreferredName!=null) {
            names.addIfNotNull(optionalDefaultPreferredName);
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
    
    public static Set<YomlSerializer> findSerializerAnnotations(Class<?> type) {
        Set<YomlSerializer> result = MutableSet.of();
        if (!type.getSuperclass().equals(Object.class)) {
            result.addAll(findSerializerAnnotations(type.getSuperclass()));
        }
        
        YomlAllFieldsAtTopLevel allFields = type.getAnnotation(YomlAllFieldsAtTopLevel.class);
        for (Field f: type.getDeclaredFields()) {
            YomlFieldAtTopLevel ytf = f.getAnnotation(YomlFieldAtTopLevel.class);
            if (ytf!=null || allFields!=null)
                result.add(new ExplicitField(f));
        }
        
        return result;
    }

}
