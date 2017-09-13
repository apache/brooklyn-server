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
package org.apache.brooklyn.core.sensor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.util.guava.TypeTokens;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

/**
 * Parent for all {@link Sensor}s.
 */
public class BasicSensor<T> implements Sensor<T> {
    private static final long serialVersionUID = -3762018534086101323L;
    
    private static final Splitter dots = Splitter.on('.');

    private TypeToken<T> typeToken;
    private Class<? super T> type;
    private String name;
    private String description;
    private transient List<String> nameParts;
    
    // constructor for json/gson (can probably be private?)
    public BasicSensor() {}

    /** name is typically a dot-separated identifier; description is optional */
    public BasicSensor(Class<T> type, String name) {
        this(type, null, name, name);
    }
    
    public BasicSensor(Class<T> type, String name, String description) {
        this(type, null, name, description);
    }
    
    @SuppressWarnings("unchecked")
    public BasicSensor(TypeToken<T> typeToken, String name, String description) {
        this((Class<T>)TypeTokens.getRawTypeIfRaw(typeToken), TypeTokens.getTypeTokenIfNotRaw(checkNotNull(typeToken, "typeToken")),
                checkNotNull(name, "name"), description);
    }
    
    protected BasicSensor(Class<T> type, TypeToken<T> typeToken, String name, String description) {
        TypeTokens.checkCompatibleOneNonNull(type, typeToken);
        this.typeToken = typeToken;
        this.type = type;
        this.name = checkNotNull(name, "name");
        this.description = description;
    }

    /** @see Sensor#getTypeToken() */
    @Override
    public TypeToken<T> getTypeToken() { return TypeTokens.getTypeToken(typeToken, type); }
    
    /** @see Sensor#getType() */
    @Override
    public Class<? super T> getType() { return TypeTokens.getRawType(typeToken, type); }
 
    /** @see Sensor#getTypeName() */
    @Override
    public String getTypeName() { 
        return getType().getName();
    }
 
    /** @see Sensor#getName() */
    @Override
    public String getName() { return name; }
 
    /** @see Sensor#getNameParts() */
    @Override
    public synchronized List<String> getNameParts() {
        if (nameParts==null) nameParts = ImmutableList.copyOf(dots.split(name));
        return nameParts; 
    }
 
    /** @see Sensor#getDescription() */
    @Override
    public String getDescription() { return description; }
    
    /** @see Sensor#newEvent(Entity, Object) */
    @Override
    public SensorEvent<T> newEvent(Entity producer, T value) {
        return new BasicSensorEvent<T>(this, producer, value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getTypeName(), name, description);
    }
 
    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        if (!(other instanceof BasicSensor)) return false;
        BasicSensor<?> o = (BasicSensor<?>) other;
        
        return Objects.equal(getTypeName(), o.getTypeName()) && Objects.equal(name, o.name) && Objects.equal(description, o.description);
    }
    
    @Override
    public String toString() {
        return String.format("Sensor: %s (%s)", name, getTypeName());
    }
}
