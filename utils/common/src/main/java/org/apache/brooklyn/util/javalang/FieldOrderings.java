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
import java.util.Comparator;

import org.apache.brooklyn.util.collections.MutableList;

import com.google.common.collect.Ordering;

public class FieldOrderings {
    public static class FieldNameComparator implements Comparator<Field> {
        private final Comparator<String> nameComparator;
        public FieldNameComparator(Comparator<String> nameComparator) { this.nameComparator = nameComparator; }
        @Override
        public int compare(Field o1, Field o2) {
            return nameComparator.compare(o1.getName(), o2.getName());
        }
    }
    public static class FieldClassComparator implements Comparator<Field> {
        private final Comparator<Class<?>> classComparator;
        public FieldClassComparator(Comparator<Class<?>> classComparator) { this.classComparator = classComparator; }
        @Override
        public int compare(Field o1, Field o2) {
            return classComparator.compare(o1.getDeclaringClass(), o2.getDeclaringClass());
        }
    }

    public static Comparator<Class<?>> SUB_BEST_CLASS_COMPARATOR = new SubbestClassComparator();
    private static class SubbestClassComparator implements Comparator<Class<?>> {
        @Override
        public int compare(Class<?> c1, Class<?> c2) {
            Class<?> cS = Reflections.inferSubbest(c1, c2);
            return (cS==c1 ? -1 : cS==c2 ? 1 : 0);
        }
    }

    /** Puts fields lower in the hierarchy first, and otherwise leaves fields in order */
    public static Comparator<Field> SUB_BEST_FIELD_FIST_THEN_DEFAULT = 
        new FieldClassComparator(SUB_BEST_CLASS_COMPARATOR);
    /** Puts fields higher in the hierarchy first, and otherwise leaves fields in order */
    public static Comparator<Field> SUB_BEST_FIELD_LAST_THEN_DEFAULT = 
        new FieldClassComparator(Ordering.from(SUB_BEST_CLASS_COMPARATOR).reverse());
    /** Puts fields that are lower down the hierarchy first, and then sorts those alphabetically */
    @SuppressWarnings("unchecked")
    public static Comparator<Field> SUB_BEST_FIELD_FIRST_THEN_ALPHABETICAL = Ordering.compound(MutableList.of(
        new FieldClassComparator(SUB_BEST_CLASS_COMPARATOR),
        new FieldNameComparator(Ordering.<String>natural())));
    /** Puts fields that are higher up in the hierarchy first, and then sorts those alphabetically */
    @SuppressWarnings("unchecked")
    public static Comparator<Field> SUB_BEST_FIELD_LAST_THEN_ALPHABETICAL = Ordering.compound(MutableList.of(
        new FieldClassComparator(Ordering.from(SUB_BEST_CLASS_COMPARATOR).reverse()),
        new FieldNameComparator(Ordering.<String>natural())));
    /** Puts fields in alpha order, but in cases of duplicate those lower down the hierarchy are first */
    @SuppressWarnings("unchecked")
    public static Comparator<Field> ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST = Ordering.compound(MutableList.of(
        new FieldNameComparator(Ordering.<String>natural()),
        new FieldClassComparator(SUB_BEST_CLASS_COMPARATOR)));
    /** Puts fields in alpha order, but in cases of duplicate those higher up in the hierarchy are first 
     * (potentially confusing, as this will put masked fields first) */
    @SuppressWarnings("unchecked")
    public static Comparator<Field> ALPHABETICAL_FIELD_THEN_SUB_BEST_LAST = Ordering.compound(MutableList.of(
        new FieldNameComparator(Ordering.<String>natural()),
        new FieldClassComparator(Ordering.from(SUB_BEST_CLASS_COMPARATOR).reverse())));
}