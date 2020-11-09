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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerialization.BrooklynJacksonType;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
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
                        // see note below, on convert()
                ).apply(mapper);

        return mapper;
    }

    public static JsonMapper newSimpleMapper() {
        // for use with json maps (no special type resolution, even the field "type" is ignored)
        return JsonMapper.builder().build();
    }

    public static boolean isPureJson(Object o) {
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
            return true;
        }
        if (o instanceof Map) {
            for (Map.Entry<?,?> oo : ((Map<?,?>) o).entrySet()) {
                if (!isJsonAndOthers(oo.getKey(), acceptOthers)) return false;
                if (!isJsonAndOthers(oo.getValue(), acceptOthers)) return false;
            }
            return true;
        }
        return acceptOthers.test(o);
    }
    public static boolean isJsonOrDeferredSupplier(Object o) {
        return isJsonAndOthers(o, oo -> oo instanceof DeferredSupplier);
    }

    @Beta
    public static class RegisteredTypeOrTypeToken<T> {
        private final TypeToken<T> tt;
        private final RegisteredType rt;
        private RegisteredTypeOrTypeToken(TypeToken<T> tt, RegisteredType rt) {
            this.tt = tt;
            this.rt = rt;
        }

        public Class<T> getRawClass() {
            if (rt!=null) {
                return (Class<T>) rt.getSuperTypes().stream().filter(i -> i instanceof Class).findAny().orElse(Object.class);
            }
            return (Class<T>) tt.getRawType();
        }
        public TypeToken<T> getTypeToken() {
            if (tt!=null) return tt;
            return TypeToken.of(getRawClass());
        }

        public Optional<RegisteredType> getRegisteredType() {
            return Optional.ofNullable(rt);
        }

        public static <T> RegisteredTypeOrTypeToken<T> of(Class<T> t) {
            return of(TypeToken.of(t));
        }

        public static <T> RegisteredTypeOrTypeToken<T> of(TypeToken<T> t) {
            return new RegisteredTypeOrTypeToken(t, null);
        }

        public static <T> RegisteredTypeOrTypeToken<T> of(RegisteredType t) {
            return new RegisteredTypeOrTypeToken(null, t);
        }
    }

    /* a lot of consideration over where bean-with-type conversion should take place.
     * it is especially handy for config and for initializers, and sometimes for values _within_ those items.
     * currently these are done:
     * - in BrooklynComponentTemplateResolver for config, initializers etc
     * - on coercion, when accessing config
     * see esp CustomTypeConfigYamlTest.
     *
     * BrooklynComponentTemplateResolver is also responsible for parsing the DSL, which it does first,
     * and DSL objects are preserved by bean-with-type transformation --
     * see in JsonDeserializerForCommonBrooklynThings.  See DslSerializationTest.
     */

    public static <T> T convert(ManagementContext mgmt, Object mapOrListToSerializeThenDeserialize, RegisteredTypeOrTypeToken<T> type, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowJavaTypes) throws JsonProcessingException {
        ObjectMapper m = newMapper(mgmt, allowRegisteredTypes, loader, allowJavaTypes);
        String serialization = m.writeValueAsString(mapOrListToSerializeThenDeserialize);
        if (type.rt!=null) {
            return m.readValue(serialization, new BrooklynJacksonType(mgmt, type.rt));
        } else {
            return m.readValue(serialization, m.constructType(type.tt.getType()));
        }
    }

    public static <T> Maybe<T> tryConvertOrAbsentUsingContext(Maybe<Object> input, RegisteredTypeOrTypeToken<T> type) {
        Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        ManagementContext mgmt = entity != null ? ((EntityInternal) entity).getManagementContext() : null;
        OsgiBrooklynClassLoadingContext loader = entity != null ? new OsgiBrooklynClassLoadingContext(entity) : null;
        return BeanWithTypeUtils.tryConvertOrAbsent(mgmt, input, type, true, loader, false);
    }

    public static <T> Maybe<T> tryConvertOrAbsent(ManagementContext mgmt, Maybe<Object> inputMap, RegisteredTypeOrTypeToken<T> type, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowJavaTypes) {
        if (inputMap.isAbsent()) return (Maybe<T>)inputMap;

        Object o = inputMap.get();
        if (!(o instanceof Map) && !(o instanceof List)) {
            if (type.getTypeToken().isAssignableFrom(o.getClass())) {
                return (Maybe<T>)inputMap;
            }  else {
                return Maybe.absent(() -> new RuntimeException("BeanWithType cannot convert from "+o.getClass()+" to "+type));
            }
        }

        Maybe<T> fallback = null;
        if (type.getTypeToken().isAssignableFrom(Object.class)) {
            // the input is already valid, so use it as the fallback result
            fallback = (Maybe<T>)inputMap;

            // there isn't a 'type' key so little obvious point in converting .. might make a difference _inside_ a map or list, but we've not got any generics so it won't
            if (!(o instanceof Map) || !((Map<?, ?>) o).containsKey("type")) return fallback;
        } else if (type.getTypeToken().isAssignableFrom(Map.class)) {
            // skip conversion for a map if it isn't an object
            return (Maybe<T>)inputMap;
        }

        try {
            return Maybe.of(convert(mgmt, o, type, allowRegisteredTypes, loader, allowJavaTypes));
        } catch (Exception e) {
            if (fallback!=null) return fallback;
            return Maybe.absent("BeanWithType cannot convert given input to "+type, e);
        }
    }

    /** Whether there appears to be an incompatibility and conversion might fix it. */
    public static <T> boolean isConversionRecommended(Maybe<Object> input, TypeToken<T> type) {
        return getPotentialConvertibilityScoreInternal(input.orNull(), type) == 1;
    }

    /** Like {@link #isConversionRecommended(Maybe, TypeToken)} but much weaker,
     * in particular if Object is expected, this will return true whereas that will return false.
     * This will return false if there are contents which are simply incompatible. */
    public static <T> boolean isConversionPlausible(Maybe<Object> input, TypeToken<T> type) {
        if (input==null || input.isAbsentOrNull() || type==null) return false;
        return getPotentialConvertibilityScoreInternal(input.get(), type) >= 0;
    }

    /** -1 if conversion _won't_ fix it. 1 if there is a problem that likely _would_ be fixed. 0 if no obvious need or problem. */
    private static <T> int getPotentialConvertibilityScoreInternal(Object t, TypeToken<T> type) {
        // if we want a primitive/string, conversion won't help (coercion would be sufficient); return 0 if type is consistent, or -1 if not
        if (t==null) {
            return 0;
        }
        if (Boxing.isPrimitiveOrBoxedClass(type.getRawType()) || String.class.equals(type.getRawType())) {
            return t.getClass().equals(type.getRawType()) ? 0 : -1;
        }
        // if we want an object, then no need for conversion nor any problem with conversion
        if (Object.class.equals(type.getRawType())) return 0;

        if (Map.class.isAssignableFrom(type.getRawType())) {
            // if map is wanting, superficially we don't want to convert; but if there are generics we need to recurse
            if (t instanceof Map) {
                List<TypeToken<?>> generics = TypeTokens.getGenericArguments(type);
                if (generics!=null) {
                    for (Entry<?, ?> entry : ((Map<?, ?>) t).entrySet()) {
                        int k = getPotentialConvertibilityScoreInternal(entry.getKey(), generics.get(0));
                        if (k!=0) return k;
                        int v = getPotentialConvertibilityScoreInternal(entry.getValue(), generics.get(1));
                        if (v!=0) return v;
                    }
                }
                return 0;
            } else {
                // conversion won't make a map from a non-map
                return -1;
            }
        }

        if (Collection.class.isAssignableFrom(type.getRawType())) {
            if (t instanceof Collection) {
                List<TypeToken<?>> generics = TypeTokens.getGenericArguments(type);
                if (generics!=null) {
                    for (Object entry : ((Collection<?>) t)) {
                        int k = getPotentialConvertibilityScoreInternal(entry, generics.get(0));
                        if (k != 0) return k;
                    }
                }
                return 0;
            } else {
                return -1;
            }
        }

        // we want some special object. if we have a map or a string then conversion might sort us out.
        return (t instanceof Map || t instanceof String) ? 1 : -1;
    }
}
