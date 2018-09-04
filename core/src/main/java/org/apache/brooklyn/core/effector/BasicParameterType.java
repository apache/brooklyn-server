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
package org.apache.brooklyn.core.effector;

import java.util.Collections;
import java.util.Map;

import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.util.core.flags.TypeCoercions;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;

public class BasicParameterType<T> implements ParameterType<T> {
    private static final long serialVersionUID = -5521879180483663919L;
    
    private String name;
    private Class<T> type;
    private TypeToken<T> typeT;
    private String description;
    private Boolean hasDefaultValue = null;
    private T defaultValue = null;

    public BasicParameterType() {
        this(Collections.emptyMap());
    }
    
    @SuppressWarnings("unchecked")
    public BasicParameterType(Map<?, ?> arguments) {
        if (arguments.containsKey("name")) name = (String) arguments.get("name");
        
        if (arguments.containsKey("typeT")) {
            Object t = arguments.get("typeT");
            typeT = TypeCoercions.coerce(t, TypeToken.class);
        } else if (arguments.containsKey("type")) {
            Object t = arguments.get("type");
            if (t instanceof Class) type = ((Class<T>)t);
            else if (t instanceof TypeToken) typeT = ((TypeToken<T>)t);
            else typeT = TypeCoercions.coerce(t, TypeToken.class);
        }
        
        if (arguments.containsKey("description")) description = (String) arguments.get("description");
        if (arguments.containsKey("defaultValue")) defaultValue = (T) arguments.get("defaultValue");
    }

    public BasicParameterType(String name, Class<T> type) {
        this(name, TypeToken.of(type), null, null, false);
    }
    
    public BasicParameterType(String name, Class<T> type, String description) {
        this(name, TypeToken.of(type), description, null, false);
    }
    
    public BasicParameterType(String name, Class<T> type, String description, T defaultValue) {
        this(name, TypeToken.of(type), description, defaultValue, true);
    }
    
    public BasicParameterType(String name, Class<T> type, String description, T defaultValue, boolean hasDefaultValue) {
        this(name, TypeToken.of(type), description, defaultValue, hasDefaultValue);
    }
    
    public BasicParameterType(String name, TypeToken<T> type) {
        this(name, type, null, null, false);
    }
    
    public BasicParameterType(String name, TypeToken<T> type, String description) {
        this(name, type, description, null, false);
    }
    
    public BasicParameterType(String name, TypeToken<T> type, String description, T defaultValue) {
        this(name, type, description, defaultValue, true);
    }
    
    @SuppressWarnings("unchecked")
    public BasicParameterType(String name, TypeToken<T> type, String description, T defaultValue, boolean hasDefaultValue) {
        this.name = name;
        if (type!=null && type.equals(TypeToken.of(type.getRawType()))) {
            // prefer Class if it's already a raw type; keeps persistence simpler (and the same as before)
            this.type = (Class<T>) type.getRawType();
        } else {
            this.typeT = type;
        }
        this.description = description;
        this.defaultValue = defaultValue;
        if (defaultValue!=null && !defaultValue.getClass().equals(Object.class)) {
            // if default value is null (or is an Object, which is ambiguous on resolution to to rebind), 
            // don't bother to set this as it creates noise in the persistence files
            this.hasDefaultValue = hasDefaultValue;
        }
    }

    @Override
    public String getName() { return name; }

    @SuppressWarnings("unchecked")
    @Override
    public Class<T> getParameterClass() {
        if (typeT!=null) return (Class<T>) typeT.getRawType();
        if (type!=null) return type;
        return null;
    }

    @Override
    public TypeToken<T> getParameterType() { 
        if (typeT!=null) return typeT;
        if (type!=null) return TypeToken.of(type);
        return null;
    }

    @Override
    public String getParameterClassName() { return getParameterType().toString(); }

    @Override
    public String getDescription() { return description; }

    @Override
    public T getDefaultValue() {
        return hasDefaultValue() ? defaultValue : null;
    }

    public boolean hasDefaultValue() {
        // a new Object() was previously used to indicate no default value, but that doesn't work well across serialization boundaries!
        return hasDefaultValue!=null ? hasDefaultValue : defaultValue!=null && !defaultValue.getClass().equals(Object.class);
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("name", name).add("description", description).add("type", getParameterClassName())
                .add("defaultValue", defaultValue)
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(name, description, getParameterType(), defaultValue);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof ParameterType) &&
                Objects.equal(name, ((ParameterType<?>)obj).getName()) &&
                Objects.equal(description, ((ParameterType<?>)obj).getDescription()) &&
                Objects.equal(getParameterType(), ((ParameterType<?>)obj).getParameterType()) &&
                Objects.equal(defaultValue, ((ParameterType<?>)obj).getDefaultValue());
    }
}
