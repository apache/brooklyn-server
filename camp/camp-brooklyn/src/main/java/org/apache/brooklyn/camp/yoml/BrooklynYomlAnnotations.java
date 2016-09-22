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
package org.apache.brooklyn.camp.yoml;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.brooklyn.camp.yoml.serializers.InstantiateTypeFromRegistryUsingConfigBag;
import org.apache.brooklyn.camp.yoml.serializers.InstantiateTypeFromRegistryUsingConfigKeyMap;
import org.apache.brooklyn.util.core.yoml.YomlConfigBagConstructor;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.YomlAnnotations;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;

public class BrooklynYomlAnnotations extends YomlAnnotations {

    public Collection<YomlSerializer> findConfigBagConstructorSerializers(Class<?> t) {
        YomlConfigBagConstructor ann = t.getAnnotation(YomlConfigBagConstructor.class);
        if (ann==null) return Collections.emptyList();
        return new InstantiateTypeFromRegistryUsingConfigBag.Factory().newConfigKeySerializersForType(
            t,
            ann.value(), ann.writeAsKey()!=null ? ann.writeAsKey() : ann.value(),
            ann.validateAheadOfTime(), ann.requireStaticKeys());
    }

    public Collection<YomlSerializer> findConfigMapConstructorSerializersIgnoringInheritance(Class<?> t) {
        throw new UnsupportedOperationException("ensure this doesn't get called");
    }
    
    public Collection<YomlSerializer> findConfigMapConstructorSerializersWithInheritance(Class<?> t) {
        YomlConfigMapConstructor ann = t.getAnnotation(YomlConfigMapConstructor.class);
        if (ann==null) return Collections.emptyList();
        return new InstantiateTypeFromRegistryUsingConfigKeyMap.Factory().newConfigKeySerializersForType(
            t,
            ann.value(), Strings.isNonBlank(ann.writeAsKey()) ? ann.writeAsKey() : ann.value(),
            ann.validateAheadOfTime(), ann.requireStaticKeys());
    }

    @Override
    protected void collectSerializersForConfig(Set<YomlSerializer> result, Class<?> type) {
        result.addAll(findConfigBagConstructorSerializers(type));
        result.addAll(findConfigMapConstructorSerializersWithInheritance(type));
    }

}
