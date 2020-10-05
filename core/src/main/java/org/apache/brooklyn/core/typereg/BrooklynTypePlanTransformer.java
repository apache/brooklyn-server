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
package org.apache.brooklyn.core.typereg;

import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;

import com.google.common.annotations.Beta;

/**
 * Interface for use by schemes which provide the capability to transform plans
 * (serialized descriptions) to brooklyn objecs and specs.
 * <p>
 * To add a new plan transformation scheme, simply create an implementation and declare it
 * as a java service (cf {@link ServiceLoader}).
 * <p>
 * Implementations may wish to extend {@link AbstractTypePlanTransformer} which simplifies the process.
 * <p>
 * See also the BrooklynCatalogBundleResolver in the core project, for adding types with plans to the type registry.
 */
public interface BrooklynTypePlanTransformer extends ManagementContextInjectable {

    /** @return An identifier for the transformer. 
     * This may be used by RegisteredType instances to target a specific transformer. */
    String getFormatCode();
    /** @return A display name for this transformer. 
     * This may be used to prompt a user what type of plan they are supplying. */
    String getFormatName();
    /** @return A description for this transformer */
    String getFormatDescription();

    /** 
     * Determines how appropriate is this transformer for the {@link RegisteredType#getPlan()} of the type.
     * The framework guarantees arguments are nonnull, and that the {@link RegisteredType#getPlan()} is also not-null.
     * However many fields on the {@link RegisteredType} may be null,
     * including {@link RegisteredType#getId()} for an ad hoc creation
     * (eg if invoked from {@link BrooklynTypeRegistry#createBeanFromPlan(String, Object, RegisteredTypeLoadingContext, Class)})
     *  
     * @return A co-ordinated score / confidence value in the range 0 to 1. 
     * 0 means not compatible, 
     * 1 means this is clearly the intended transformer and no others need be tried 
     * (for instance because the format is explicitly specified),
     * and values between 0 and 1 indicate how likely a transformer believes it should be used.
     * <p>
     * Values greater than 0.5 are generally reserved for the presence of marker tags or files
     * which strongly indicate that the format is compatible.
     * Such a value should be returned even if the plan is not actually parseable, but if it looks like a user error
     * which prevents parsing (eg mal-formed YAML) and the transformer could likely be the intended target.
     * <p>
     * */
    double scoreForType(@Nonnull RegisteredType type, @Nonnull RegisteredTypeLoadingContext context);
    /** Creates a new instance of the indicated type, or throws if not supported;
     * this method is used by the {@link BrooklynTypeRegistry} when it creates instances,
     * so implementations must respect the {@link RegisteredTypeKind} semantics and the {@link RegisteredTypeLoadingContext}
     * (or return null / throw).
     * <p>
     * The framework guarantees this will only be invoked when {@link #scoreForType(RegisteredType, RegisteredTypeLoadingContext)} 
     * has returned a positive value, and the same constraints on the inputs as for that method apply.
     * <p>
     * Implementations should either return null or throw {@link UnsupportedTypePlanException} 
     * if upon closer inspection following a non-null score, they do not actually support the given {@link RegisteredType#getPlan()}.
     * If they should support the plan but the plan contains an error, they should throw the relevant error for feedback to the user. */
    @Nullable Object create(@Nonnull RegisteredType type, @Nonnull RegisteredTypeLoadingContext context);

    /** @deprecated since 1.0; use {@link org.apache.brooklyn.core.typereg.BrooklynCatalogBundleResolver} for adding to catalog */
    @Deprecated
    default double scoreForTypeDefinition(String formatCode, Object catalogData) { return 0; }
    /** @deprecated since 1.0; use {@link org.apache.brooklyn.core.typereg.BrooklynCatalogBundleResolver} for adding to catalog */
    @Deprecated
    default List<RegisteredType> createFromTypeDefinition(String formatCode, Object catalogData) { return null; }

}
