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
package org.apache.brooklyn.camp.yoml.serializers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;

@Alias("config-bag-constructor")
public class InstantiateTypeFromRegistryUsingConfigBag extends InstantiateTypeFromRegistryUsingConfigMap {

    public static class Factory extends InstantiateTypeFromRegistryUsingConfigMap.Factory {
        protected InstantiateTypeFromRegistryUsingConfigBag newInstance() {
            return new InstantiateTypeFromRegistryUsingConfigBag();
        }
    }
    
    protected Maybe<?> findConstructorMaybe(Class<?> type) {
        Maybe<?> c = findConfigBagConstructor(type);
        if (c.isPresent()) return c;
        Maybe<?> c2 = Reflections.findConstructorMaybe(type, Map.class);
        if (c2.isPresent()) return c2;
        
        return c;
    }
    protected Maybe<?> findConfigBagConstructor(Class<?> type) {
        return Reflections.findConstructorMaybe(type, ConfigBag.class);
    }
    protected Maybe<Field> findFieldMaybe(Class<?> type) {
        Maybe<Field> f = Reflections.findFieldMaybe(type, fieldNameForConfigInJavaIfPreset);
        if (f.isPresent() && !(Map.class.isAssignableFrom(f.get().getType()) || ConfigBag.class.isAssignableFrom(f.get().getType()))) 
            f = Maybe.absent();
        return f;
    }

    @Override
    protected Map<String, Object> getRawConfigMap(Field f, Object obj) throws IllegalAccessException {
        if (ConfigBag.class.isAssignableFrom(f.getType())) {
            return ((ConfigBag)f.get(obj)).getAllConfig();
        }
        return super.getRawConfigMap(f, obj);
    }

    @Override
    protected ConstructionInstruction newConstructor(Class<?> type, Map<String, Object> fieldsFromReadToConstructJava, ConstructionInstruction optionalOuter) {
        Maybe<?> constructor = findConfigBagConstructor(type);
        if (constructor.isPresent() && ConfigBag.class.isAssignableFrom( (((Constructor<?>)constructor.get()).getParameterTypes()[0]) )) {
            return ConstructionInstructions.Factory.newUsingConstructorWithArgs(type, MutableList.of(
                ConfigBag.newInstance(fieldsFromReadToConstructJava)), optionalOuter);
        }
        return super.newConstructor(type, fieldsFromReadToConstructJava, optionalOuter);
    }

}
