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
package org.apache.brooklyn.util.javalang.coerce;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Enums;
import org.apache.brooklyn.util.javalang.JavaClassNames;

import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public class EnumTypeCoercions {

    /**
     * Type coercion {@link Function function} for {@link Enum enums}.
     * <p>
     * Tries to convert the string to {@link CaseFormat#UPPER_UNDERSCORE} first,
     * handling all of the different {@link CaseFormat format} possibilites. Failing 
     * that, it tries a case-insensitive comparison with the valid enum values.
     * <p>
     * Returns {@code defaultValue} if the string cannot be converted.
     *
     * @see TypeCoercions#coerce(Object, Class)
     * @see Enum#valueOf(Class, String)
     */
    public static <E extends Enum<E>> Function<String, E> stringToEnum(final Class<E> type, @Nullable final E defaultValue) {
        return new StringToEnumFunction<E>(type, defaultValue);
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> Function<String, T> stringToEnumUntyped(final Class<? super T> type, @Nullable final T defaultValue) {
        if (!type.isEnum()) return new Functionals.ConstantFunction<String,T>(null);
        return new StringToEnumFunction((Class<Enum>)type, (Enum)defaultValue);
    }
    
    private static class StringToEnumFunction<E extends Enum<E>> implements Function<String, E> {
        private final Class<E> type;
        private final E defaultValue;
        
        public StringToEnumFunction(Class<E> type, @Nullable E defaultValue) {
            this.type = type;
            this.defaultValue = defaultValue;
        }
        @Override
        public E apply(String input) {
            return tryCoerce(input, type).or(defaultValue);
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> Maybe<T> tryCoerceUntyped(String input, Class<T> targetType) {
        if (input==null) return null;
        if (targetType==null) return Maybe.absent("Null enum type");
        if (!targetType.isEnum()) return Maybe.absent("Type '"+targetType+"' is not an enum");
        return tryCoerce(input, (Class<Enum>)targetType);
    }
    
    public static <E extends Enum<E>> Maybe<E> tryCoerce(String input, Class<E> targetType) {
        if (input==null) return null;
        if (targetType==null) return Maybe.absent("Null enum type");
        if (!targetType.isEnum()) return Maybe.absent("Type '"+targetType+"' is not an enum");
        
        List<String> options = ImmutableList.of(
                input,
                CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_UNDERSCORE, input),
                CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_UNDERSCORE, input),
                CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, input),
                CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, input));
        for (String value : options) {
            try {
                return Maybe.of(Enum.valueOf(targetType, value));
            } catch (IllegalArgumentException iae) {
                continue;
            }
        }
        Maybe<E> result = Enums.valueOfIgnoreCase(targetType, input);
        if (result.isPresent()) return result;
        return Maybe.absent(new ClassCoercionException("Invalid value '"+input+"' for "+JavaClassNames.simpleClassName(targetType)+"; expected one of "+
            Arrays.asList(Enums.values(targetType))));
    }
    
}
