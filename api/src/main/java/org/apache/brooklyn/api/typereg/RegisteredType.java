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
package org.apache.brooklyn.api.typereg;

import java.util.Collection;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Identifiable;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.util.osgi.VersionedName;

import com.google.common.annotations.Beta;

public interface RegisteredType extends Identifiable {
    
    @Override String getId();
    
    RegisteredTypeKind getKind();
    
    String getSymbolicName();
    String getVersion();
    
    VersionedName getVersionedName();

    /** Bundle in symbolicname:id format where this type is defined */
    // TODO would prefer this to be VersionedName if/when everything comes from OSGi bundles
    // unrevert 7260bf9cf3f3ebaaa790693e1b7217a81bef78a7 to start that, and adjust serialization
    // as described in that commit message (supporting String in xstream serialization for VN)
    String getContainingBundle();

    /** Library bundle search path that this item declares for resolving types -
     * registered types from these bundles should be preferred,  
     * java types from these bundles will be accessible during construction,
     * and if the resulting instance maintains a search path these bundles will be declared there.
     * <p>
     * This does not include bundles from parent registered types for this item 
     * (unless the designer re-declares them explicitly), but typical implementations
     * of instantiation will find those at construction time and if the resulting instance 
     * maintains a search path those items will typically be added there.
     */
    Collection<OsgiBundleWithUrl> getLibraries();

    String getDisplayName();
    String getDescription();
    String getIconUrl();

    /** @return all declared supertypes or super-interfaces of this registered type,
     * consisting of a collection of {@link Class} or {@link RegisteredType}
     * <p>
     * This should normally include at least one {@link Class} object for filtering purposes:
     * For beans, this should include the java type that the {@link BrooklynTypeRegistry} will create. 
     * For specs, this should refer to the {@link BrooklynObject} type that the created spec will point at 
     * (e.g. the concrete {@link Entity}, not the {@link EntitySpec}).
     * <p>
     * For specs, this will normally return the most specific java interface it will create (e.g. BasicEntity),
     * then the category (e.g. Entity), then other ancestor classes/interfaces.
     * Registered supertypes and potentially marker registered type interfaces may also be included.
     * <p>
     * In some places, e.g. for items not yet validated or fully loaded,
     * this list may be incomplete, but it should normally for specs include at least the category the spec will create.
     * <p>
     * This may include multiple interfaces but exclude the concrete subclass which implements them all
     * (for instance if that concrete implementation is an internal private class). 
     * However it must be possible for the corresponding transformer to instantiate that type at runtime.
     * (It will never return two classes which cannot have a common subclass.)
     * <p>
     * Any {@link RegisteredType} instances returned by this method may be stale.
     * (TODO Currently I don't think it ever includes {@link RegisteredType} supertypes.) 
     */
    @Beta
    Set<Object> getSuperTypes();

    /**
     * @return True if the item has been deprecated (i.e. its use is discouraged)
     */
    boolean isDeprecated();
    
    /**
     * @return True if the item has been disabled (i.e. its use is forbidden, except for pre-existing apps)
     */
    boolean isDisabled();

    /** Alias words defined for this type */
    Set<String> getAliases();

    /** Tags attached to this item */
    Set<Object> getTags();
    
    /** @return implementation details, so that the framework can find a suitable {@link BrooklynTypePlanTransformer} 
     * which can then use this object to instantiate this type */
    TypeImplementationPlan getPlan();
    
    public interface TypeImplementationPlan {
        /** hint which {@link BrooklynTypePlanTransformer} instance(s) can be used, if known;
         * this may be null if the relevant transformer was not declared when created,
         * but in general we should look to determine the kind as early as possible 
         * and use that to retrieve the appropriate such transformer */
        @Nullable 
        String getPlanFormat();
        /** data for the implementation; may be more specific */
        Object getPlanData(); // TODO unclear if this is allowed to return null; most (?) usages do a null check
        
        @Override boolean equals(Object obj);
        @Override int hashCode();
    }

    @Override boolean equals(Object obj);
    @Override int hashCode();
}
