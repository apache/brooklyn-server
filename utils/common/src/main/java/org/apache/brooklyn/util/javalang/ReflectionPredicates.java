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
package org.apache.brooklyn.util.javalang;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class ReflectionPredicates {
    
    public static Predicate<Integer> MODIFIERS_PRIVATE = new ModifiersPrivate();
    private static class ModifiersPrivate implements Predicate<Integer> {
        @Override public boolean apply(Integer modifiers) { return Modifier.isPrivate(modifiers); }
    }
    public static Predicate<Integer> MODIFIERS_PUBLIC = new ModifiersPublic();
    private static class ModifiersPublic implements Predicate<Integer> {
        @Override public boolean apply(Integer modifiers) { return Modifier.isPublic(modifiers); }
    }
    public static Predicate<Integer> MODIFIERS_PROTECTED = new ModifiersProtected();
    private static class ModifiersProtected implements Predicate<Integer> {
        @Override public boolean apply(Integer modifiers) { return Modifier.isProtected(modifiers); }
    }
    
    public static Predicate<Integer> MODIFIERS_TRANSIENT = new ModifiersTransient();
    private static class ModifiersTransient implements Predicate<Integer> {
        @Override public boolean apply(Integer modifiers) { return Modifier.isTransient(modifiers); }
    }
    public static Predicate<Integer> MODIFIERS_STATIC = new ModifiersStatic();
    private static class ModifiersStatic implements Predicate<Integer> {
        @Override public boolean apply(Integer modifiers) { return Modifier.isStatic(modifiers); }
    }
    
    public static Predicate<Field> fieldModifiers(Predicate<Integer> modifiersCheck) { return new FieldModifiers(modifiersCheck); }
    private static class FieldModifiers implements Predicate<Field> {
        private Predicate<Integer> modifiersCheck;
        private FieldModifiers(Predicate<Integer> modifiersCheck) { this.modifiersCheck = modifiersCheck; }
        @Override public boolean apply(Field f) { return modifiersCheck.apply(f.getModifiers()); }
    }
    public static Predicate<Field> IS_FIELD_PUBLIC = fieldModifiers(MODIFIERS_PUBLIC);
    public static Predicate<Field> IS_FIELD_TRANSIENT = fieldModifiers(MODIFIERS_TRANSIENT);
    public static Predicate<Field> IS_FIELD_NON_TRANSIENT = Predicates.not(IS_FIELD_TRANSIENT);
    public static Predicate<Field> IS_FIELD_STATIC = fieldModifiers(MODIFIERS_STATIC);
    public static Predicate<Field> IS_FIELD_NON_STATIC = Predicates.not(IS_FIELD_STATIC);

    public static Predicate<Method> methodModifiers(Predicate<Integer> modifiersCheck) { return new MethodModifiers(modifiersCheck); }
    private static class MethodModifiers implements Predicate<Method> {
        private Predicate<Integer> modifiersCheck;
        private MethodModifiers(Predicate<Integer> modifiersCheck) { this.modifiersCheck = modifiersCheck; }
        @Override public boolean apply(Method m) { return modifiersCheck.apply(m.getModifiers()); }
    }

}