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
package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

public class BeanWithTypeUtils {

    public static final String FORMAT = "bean-with-type";

    public static ObjectMapper newMapper(ManagementContext mgmt, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowBasicJavaTypes) {
        JsonMapper mapper = newSimpleMapper();

        BrooklynRegisteredTypeJacksonSerialization.apply(mapper, mgmt, allowRegisteredTypes, loader, allowBasicJavaTypes);
        WrappedValuesSerialization.apply(mapper);
        mapper = new ConfigurableBeanDeserializerModifier()
                .addDeserializerWrapper(
                        d -> new JsonDeserializerForCommonBrooklynThings(mgmt, d)
                        //TODO DslSerializationAsToString - see CustomTypeConfigYamlTest
                ).apply(mapper);

        return mapper;
    }

    public static JsonMapper newSimpleMapper() {
        // for use with json maps (no special type resolution, even the field "type" is ignored)
        return JsonMapper.builder().build();
    }

    public static boolean isPureJsonX(Object o) {
        return isJsonAndOthers(o, oo -> false);
    }
    public static boolean isJsonAndOthers(Object o, Predicate<Object> acceptOthers) {
        if (o instanceof String) return true;
        if (o==null) return true;
        if (Boxing.isPrimitiveOrBoxedObject(o)) return true;
        if (o instanceof Collection) {
            for (Object oo : (Collection<?>) o) {
                if (!isJsonAndOthers(oo, acceptOthers)) return false;
            }
        }
        if (o instanceof Map) {
            for (Map.Entry<?,?> oo : ((Map<?,?>) o).entrySet()) {
                if (!isJsonAndOthers(oo.getKey(), acceptOthers)) return false;
                if (!isJsonAndOthers(oo.getValue(), acceptOthers)) return false;
            }
        }
        return acceptOthers.test(o);
    }
    public static boolean isJsonOrDeferredSupplier(Object o) {
        return isJsonAndOthers(o, oo -> oo instanceof DeferredSupplier);
    }

    public static <T> T convert(ManagementContext mgmt, Map<?,?> map, TypeToken<T> type, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowJavaTypes) throws JsonProcessingException {
        ObjectMapper m = newMapper(mgmt, allowRegisteredTypes, loader, allowJavaTypes);
        return m.readValue(m.writeValueAsString(map), BrooklynJacksonSerializationUtils.asTypeReference(type));
    }

    public static <T> Maybe<T> tryConvertOrAbsent(ManagementContext mgmt, Maybe<Object> inputMap, TypeToken<T> type, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowJavaTypes) {
        if (inputMap.isAbsent()) return (Maybe<T>)inputMap;

        Object o = inputMap.get();
        if (!(o instanceof Map)) {
            if (type.isAssignableFrom(o.getClass())) {
                return (Maybe<T>)inputMap;
            }  else {
                return Maybe.absent(() -> new RuntimeException("BeanWithType cannot convert from "+o.getClass()+" to "+type));
            }
        }

        Maybe<T> fallback = null;
        if (type.isAssignableFrom(Object.class)) {
            // returning the input is valid
            fallback = (Maybe<T>)inputMap;
            // and if there isn't a 'type' key there is no point in converting
            if (!((Map<?, ?>) o).containsKey("type")) return fallback;
        } else if (type.isAssignableFrom(Map.class)) {
            // skip conversion for a map if it isn't an object
            return (Maybe<T>)inputMap;
        }

        try {
            return Maybe.of(convert(mgmt, (Map<?,?>)o, type, allowRegisteredTypes, loader, allowJavaTypes));
        } catch (Exception e) {
            if (fallback!=null) return fallback;
            return Maybe.absent("BeanWithType cannot convert given map to "+type, e);
        }
    }

}
