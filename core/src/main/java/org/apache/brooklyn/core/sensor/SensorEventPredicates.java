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

import javax.annotation.Nullable;

import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.util.guava.SerializablePredicate;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class SensorEventPredicates {

    public static Predicate<SensorEvent<?>> sensorEqualTo(final Sensor<?> val) {
        return sensorSatisfies(Predicates.<Sensor<?>>equalTo(val));
    }
    
    public static Predicate<SensorEvent<?>> sensorSatisfies(final Predicate<? super Sensor<?>> condition) {
        return new SensorSatisfies(condition);
    }
    
    protected static class SensorSatisfies implements SerializablePredicate<SensorEvent<?>> {
        private static final long serialVersionUID = -3585200249520308941L;
        
        protected final Predicate<? super Sensor<?>> condition;
        protected SensorSatisfies(Predicate<? super Sensor<?>> condition) {
            this.condition = condition;
        }
        @Override
        public boolean apply(@Nullable SensorEvent<?> input) {
            return (input != null) && condition.apply(input.getSensor());
        }
        @Override
        public String toString() {
            return "sensorSatisfies("+condition+")";
        }
    }

    public static <T> Predicate<SensorEvent<T>> valueEqualTo(final T val) {
        return valueSatisfies(Predicates.equalTo(val));
    }
    
    public static <T> Predicate<SensorEvent<T>> valueSatisfies(final Predicate<? super T> condition) {
        return new ValueSatisfies<T>(condition);
    }
    
    protected static class ValueSatisfies<T> implements SerializablePredicate<SensorEvent<T>> {
        private static final long serialVersionUID = 2805443606039228221L;
        
        protected final Predicate<? super T> condition;
        protected ValueSatisfies(Predicate<? super T> condition) {
            this.condition = condition;
        }
        @Override
        public boolean apply(@Nullable SensorEvent<T> input) {
            return (input != null) && condition.apply(input.getValue());
        }
        @Override
        public String toString() {
            return "valueSatisfies("+condition+")";
        }
    }
}
