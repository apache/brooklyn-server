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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.guava.Maybe;

/** Provides a reference to a Brooklyn {@link RegisteredType} in the Jackson {@link com.fasterxml.jackson.databind.JavaType} hierearchy,
 * analogous to the {@link TypeToken} hierarchy but different, implementing Java's {@link Type and allowing extending,
 * and it can be used as an argument to create a TypeToken .  */
public class BrooklynJacksonType extends SimpleType implements TypeVariable<GenericDeclaration> {

    private final RegisteredType type;

    BrooklynJacksonType(RegisteredType type) {
        super(pickSuperType(type));
        this.type = type;
    }

    public RegisteredType getRegisteredType() {
        return type;
    }

    @Override
    public String getTypeName() {
        return type.getId();
    }

    @Override
    public String toString() {
        return "BrooklynJacksonType{" + type.getId() + '/' + _class + "}";
    }

    @Override
    public Type[] getBounds() {
        return new Type[0];
    }

    @Override
    public GenericDeclaration getGenericDeclaration() {
        return new FakeGenericDeclaration();
    }

    @Override
    public String getName() {
        return getTypeName();
    }

    @Override
    public AnnotatedType[] getAnnotatedBounds() {
        return new AnnotatedType[0];
    }

    @Override
    public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
        return null;
    }

    @Override
    public Annotation[] getAnnotations() {
        return new Annotation[0];
    }

    @Override
    public Annotation[] getDeclaredAnnotations() {
        return new Annotation[0];
    }

    private static class FakeGenericDeclaration implements GenericDeclaration {
        @Override
        public TypeVariable<?>[] getTypeParameters() {
            return new TypeVariable[0];
        }

        @Override
        public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
            return null;
        }

        @Override
        public Annotation[] getAnnotations() {
            return new Annotation[0];
        }

        @Override
        public Annotation[] getDeclaredAnnotations() {
            return new Annotation[0];
        }

        @Override
        public boolean equals(Object obj) {
            // allow it to equal anything so that resolution prefers this
            return true;
        }
    }


    // utilities...

    public static <T> Maybe<RegisteredType> getRegisteredType(TypeToken<T> tt) {
        Type type = tt.getType();
        if (type instanceof BrooklynJacksonType) {
            return Maybe.ofDisallowingNull( ((BrooklynJacksonType) type).getRegisteredType() );
        }
        return Maybe.absent();
    }

    public static <T> boolean isRegisteredType(TypeToken<T> tt) {
        return getRegisteredType(tt).isPresent();
    }


    // TODO could be improved to take most specific, not first; and there are probably places that duplicate this which should call here
    @Beta
    public static Class<?> pickSuperType(RegisteredType t) {
        for (Object x : t.getSuperTypes()) {
            if (x instanceof Class) return (Class<?>) x;
        }
        return Object.class;
    }

    public static BrooklynJacksonType of(RegisteredType rt) {
        return new BrooklynJacksonType(rt);
    }

    public static <T> TypeReference<T> asTypeReference(ManagementContext mgmt, String rtName) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                RegisteredType rt = mgmt.getTypeRegistry().get(rtName);
                if (rt==null) {
                    throw new IllegalStateException("Unknown type '"+rtName+"'");
                }
                return new BrooklynJacksonType(rt);
            }
        };
    }

    public static <T> TypeReference<T> asTypeReference(RegisteredType rt) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return new BrooklynJacksonType(rt);
            }
        };
    }

    public static <T> TypeReference<T> asTypeReference(TypeToken<T> typeToken) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return typeToken.getType();
            }
        };
    }

    public static <T> JavaType asJavaType(ObjectMapper m, TypeToken<T> tt) {
        Type type = tt.getType();
        if (type instanceof BrooklynJacksonType) {
            return (BrooklynJacksonType) type;
        }
        return m.constructType(tt.getType());
    }

    public static <T> TypeToken<T> asTypeToken(RegisteredType rt) {
        return (TypeToken<T>) TypeToken.of(BrooklynJacksonType.of(rt));
    }
}
