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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.javalang.Boxing;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DslCopyHelpers {

    public static DslVisitationHelper visiting(Function<BrooklynDslDeferredSupplier<?>,BrooklynDslDeferredSupplier<?>> visitor, Object ...nestedItems) {
        return new DslVisitationHelper(visitor).visiting(nestedItems);
    }

    public static <T extends BrooklynDslDeferredSupplier<?>> BrooklynDslDeferredSupplier<?> applyModificationVisitor(
            T parent, Function<BrooklynDslDeferredSupplier<?>,BrooklynDslDeferredSupplier<?>> visitor,
            Function<DslCopyHelpers.DslVisitationHelper,T> factoryIfChanged,
            Object... nestedItems) {
        return DslCopyHelpers.visiting(visitor, nestedItems).reconstructIfNecessaryAndVisit(parent, factoryIfChanged);
    }

    public static class DslVisitationHelper {

        private final Function<BrooklynDslDeferredSupplier<?>,BrooklynDslDeferredSupplier<?>> visitor;

        Map<Object,Object> visitedItems = MutableMap.of();
        boolean isChanged = false;

        public DslVisitationHelper(Function<BrooklynDslDeferredSupplier<?>, BrooklynDslDeferredSupplier<?>> visitor) {
            this.visitor = visitor;
        }

        public boolean isChanged() {
            return isChanged;
        }

        public <T> T resultOfVisiting(T item) {
            return (T) visitedItems.get(item);
        }
        public <T> T r(T item) { return resultOfVisiting(item); }

        public DslVisitationHelper visiting(Object ...nestedItems) {
            for (Object ni: nestedItems) {
                Object n2 = visitWithModificationVisitorRecursively(ni);
                visitedItems.put(ni, n2);
                if (ni!=n2) isChanged = true;
            }
            return this;
        }

        private Object visitWithModificationVisitorRecursively(Object ni) {
            return visitRecursively(ni, BrooklynDslDeferredSupplier.class, x -> x.applyModificationVisitor(visitor));
        }

        public <T extends BrooklynDslDeferredSupplier<?>> BrooklynDslDeferredSupplier<?> reconstructIfNecessaryAndVisit(T existingItem, Function<DslVisitationHelper,T> factoryIfChanged) {
            T n2 = isChanged ? factoryIfChanged.apply(this) : existingItem;
            return visitor.apply(n2);
        }
    }

    public static <T> Object visitRecursively(Object ni, Class<T> targetType, Function<T,T> visitor) {
        return visitRecursively(ni, targetType, visitor, MutableMap.of(), MutableMap.of());
    }
    private static <T> Object visitRecursively(Object ni, Class<T> targetType, Function<T,T> visitor, Map<Object,Object> visiting, Map<Object,Object> visited) {
        if (ni==null) return null;
        if (visiting.containsKey(ni)) return visiting.get(ni);  // for collections which might be modified subsequently
        if (visited.containsKey(ni)) return visited.get(ni);

        if (targetType.isInstance(ni)) return visitor.apply((T)ni);
        if (Boxing.isPrimitiveOrBoxedObject(ni) || ni instanceof String || ni instanceof Enum) return ni;

        if (ni instanceof Collection) {
            AtomicBoolean changed = new AtomicBoolean(false);
            Collection result = ni instanceof Set ? MutableSet.of() : MutableList.of();
            visiting.put(ni, result);
            ((Collection) ni).stream().forEach(x -> {
                Object x2 = visitRecursively(x, targetType, visitor, visiting, visited);
                if (x != x2) changed.set(true);
                result.add(x2);
            });
            Object resultMaybeUnchanged = changed.get() ? result : ni;
            visited.put(ni, resultMaybeUnchanged);
            visiting.remove(ni);
            return resultMaybeUnchanged;
        }
        if (ni instanceof Map) {
            AtomicBoolean changed = new AtomicBoolean(false);
            final Map result = MutableMap.of();
            visiting.put(ni, result);
            ((Map<?,?>) ni).entrySet().stream().forEach(entry -> {
                Object k2 = visitRecursively(entry.getKey(), targetType, visitor, visiting, visited);
                Object v2 = visitRecursively(entry.getValue(), targetType, visitor, visiting, visited);
                result.put(k2, v2);
                if (entry.getKey() != k2 || entry.getValue() != v2) changed.set(true);
            });
            Object resultMaybeUnchanged = changed.get() ? result : ni;
            visited.put(ni, resultMaybeUnchanged);
            visiting.remove(ni);
            return resultMaybeUnchanged;
        }
        if (ni.getClass().isArray()) {
            List<Object> l1 = Arrays.asList((Object[]) ni);
            Object[] result = l1.toArray();
            visiting.put(ni, result);

            Object l2 = visitRecursively(l1, targetType, visitor, visiting, visited);
            visiting.remove(ni);
            if (l1==l2) {
                visited.put(ni, ni);
                return ni;
            }
            // if it was changed, the array might have been accessed so we have to update its contents
            if (result.length != ((List)l2).size()) {
                // this could be supported if ever required
                throw new IllegalStateException("Cannot modify array length during visitation");
            }
            for (int i=0; i<result.length && i<((List)l2).size(); i++) {
                result[i] = ((List)l2).get(i);
            }
            visited.put(ni, result);
            return result;
        }
        return ni;
    }
}
