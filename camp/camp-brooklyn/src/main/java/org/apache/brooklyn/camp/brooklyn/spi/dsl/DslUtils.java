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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.util.Arrays;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatform;
import org.apache.brooklyn.camp.brooklyn.spi.creation.BrooklynComponentTemplateResolver;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.yaml.Yamls;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class DslUtils {

    /** true iff none of the args are deferred / tasks */
    public static boolean resolved(final Object... args) {
        if (args == null) return true;
        return resolved(Arrays.asList(args));
    }
    
    /** true iff none of the args are deferred / tasks */
    public static boolean resolved(Iterable<?> args) {
        if (args == null) return true;
        boolean allResolved = true;
        for (Object arg: args) {
            if (arg instanceof DeferredSupplier<?>) {
                allResolved = false;
                break;
            }
        }
        return allResolved;
    }
    
    private static Object transformSpecialFlags(ManagementContext mgmt, AbstractBrooklynObjectSpec<?,?> spec, Object v) {
        return new BrooklynComponentTemplateResolver.SpecialFlagsTransformer(
            CatalogUtils.newClassLoadingContext(mgmt, spec.getCatalogItemId(), ImmutableList.of()),
            MutableSet.of()).apply(v);
    }

    /** resolve an object which might be (or contain in a map or list) a $brooklyn DSL string expression */
    private static Optional<Object> resolveBrooklynDslValueInternal(Object originalValue, @Nullable TypeToken<?> desiredType, @Nullable ManagementContext mgmt, @Nullable AbstractBrooklynObjectSpec<?,?> specForCatalogItemIdContext, boolean requireType) {
        if (originalValue == null) {
            return Optional.absent();
        }
        Object value = originalValue;
        if (mgmt!=null) {
            if (value instanceof String && ((String)value).matches("\\$brooklyn:[A-Za-z_]+:\\s(?s).*")) {
                // input is a map as a string, parse it as yaml first
                value = Iterables.getOnlyElement( Yamls.parseAll((String)value) );
            }
            
            value = parseBrooklynDsl(mgmt, value);
            // TODO if it fails log a warning -- eg entitySpec with root.war that doesn't exist

            if (specForCatalogItemIdContext!=null) {
                value = transformSpecialFlags(mgmt, specForCatalogItemIdContext, value);
            }
        }
        
        if (!requireType && value instanceof DeferredSupplier) {
            // Don't cast - let Brooklyn evaluate it later (the value is a resolved DSL expression).
            return Optional.of(value);
        }
        
        if (desiredType!=null) {
            // coerce to _raw_ type as per other config-setting coercion; config _retrieval_ does type correctness
            return Optional.of(TypeCoercions.coerce(value, desiredType));

        } else {
            return Optional.of(value);
        }
    }

    /** Parses a Brooklyn DSL expression, returning a Brooklyn DSL deferred supplier if appropriate; otherwise the value is unchanged. Will walk maps/lists.  */
    // our code uses CAMP PDP to evaluate this, which is probably unnecessary, but it means the mgmt context is required and CAMP should be installed
    public static Object parseBrooklynDsl(ManagementContext mgmt, Object value) {
        // The 'dsl' key is arbitrary, but the interpreter requires a map
        ImmutableMap<String, Object> inputToPdpParse = ImmutableMap.of("dsl", value);
        Map<String, Object> resolvedConfigMap = BrooklynCampPlatform.findPlatform(mgmt)
                .pdp()
                .applyInterpreters(inputToPdpParse);
        value = resolvedConfigMap.get("dsl");
        return value;
    }

    /** Resolve an object which might be (or contain in a map or list or inside a json string) a $brooklyn DSL string expression,
     * and if a type is supplied, attempting to resolve/evaluate/coerce (unless requested type is itself a {@link DeferredSupplier}).
     * If type is not supplied acts similar to {@link #parseBrooklynDsl(ManagementContext, Object)} but with more support for
     * looking within json strings of maps/lists. */
    public static Optional<Object> resolveBrooklynDslValue(Object originalValue, @Nullable TypeToken<?> desiredType, @Nullable ManagementContext mgmt, @Nullable AbstractBrooklynObjectSpec<?,?> specForCatalogItemIdContext) {
        return resolveBrooklynDslValueInternal(originalValue, desiredType, mgmt, specForCatalogItemIdContext, false);
        
    }
    
    /** As {@link #resolveBrooklynDslValue(Object, TypeToken, ManagementContext, AbstractBrooklynObjectSpec)}
     * but returning absent if the object is DeferredSupplier, ensuring type correctness.
     * This is particularly useful for maps and other objects which might contain deferred suppliers but won't be one. */
    @SuppressWarnings("unchecked")
    public static <T> Optional<T> resolveNonDeferredBrooklynDslValue(Object originalValue, @Nullable TypeToken<T> desiredType, @Nullable ManagementContext mgmt, @Nullable AbstractBrooklynObjectSpec<?,?> specForCatalogItemIdContext) {
        return (Optional<T>) resolveBrooklynDslValueInternal(originalValue, desiredType, mgmt, specForCatalogItemIdContext, true);
    }
}
