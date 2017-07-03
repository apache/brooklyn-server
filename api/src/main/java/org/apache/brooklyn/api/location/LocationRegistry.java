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
package org.apache.brooklyn.api.location;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.LocationManager;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.util.guava.Maybe;

/**
 * The registry of the sorts of locations that brooklyn knows about. Given a
 * {@LocationDefinition} or a {@link String} representation of a spec, this can
 * be used to create a {@link Location} instance.
 */
public interface LocationRegistry {

    /** map of ID (possibly randomly generated) to the definition (spec, name, id, and props; 
     * where spec is the spec as defined, for instance possibly another named:xxx location).
     * optionally will also return all things from other sources where this can look up. */
    public Map<String,LocationDefinition> getDefinedLocations(boolean includeThingsWeAreFacadeFor);
    
    /** @deprecated since 0.12.0 use {@link #getDefinedLocations(boolean)} passing <code>true</code>.
     * some clients might only want the things actually defined here, which is cheaper. */
    @Deprecated
    public Map<String,LocationDefinition> getDefinedLocations();
    
    /** returns a LocationDefinition given its ID (usually a random string), or null if none */
    public LocationDefinition getDefinedLocationById(String id);
    
    /** returns a LocationDefinition given its name (e.g. for named locations, supply the bit after the "named:" prefix), 
     * looking inthe {@link BrooklynTypeRegistry} for registered items, then in this list, or null if none */
    public LocationDefinition getDefinedLocationByName(String name);

    /** as {@link #updateDefinedLocationNonPersisted(LocationDefinition)}
     * @deprecated since 0.12.0 use {@link #updateDefinedLocationNonPersisted(LocationDefinition)};
     * it's exactly the same, just the name makes it clear */
    @Deprecated
    public void updateDefinedLocation(LocationDefinition l);

    /** adds or updates the given defined location in this registry; note it is not persisted;
     * callers should consider adding to the {@link BrooklynTypeRegistry} instead */
    public void updateDefinedLocationNonPersisted(LocationDefinition l);

    /** removes the defined location from the registry (applications running there are unaffected) */
    public void removeDefinedLocation(String id);

    /** Returns a fully populated (config etc) location from the given definition, with optional add'l flags.
     * the location will be managed by default, unless the manage parameter is false, 
     * or the manage parameter is null and the CREATE_UNMANAGED flag is set.
     * <p>
     * The manage parameter is {@link Boolean} so that null can be used to say rely on anything in the flags.
     * 
     * @since 0.7.0, but beta and likely to change as the semantics of this class are tuned 
     * @deprecated since 0.9.0 use {@link #getLocationSpec(String, Map)} or {@link #getLocationManaged(String, Map)} */
    @SuppressWarnings("rawtypes")
    @Deprecated
    public Maybe<Location> resolve(LocationDefinition ld, Boolean manage, Map locationFlags);
    
    /** As {@link #resolve(LocationDefinition, Boolean, Map), with the location managed, and no additional flags,
     * unwrapping the result (throwing if not resolvable) 
     * @deprecated since 0.9.0 use {@link #getLocationSpec(LocationDefinition, Map)} and then manage it as needed*/
    @Deprecated
    public Location resolve(LocationDefinition l);

    /** Returns a location created from the given spec, which might correspond to a definition, or created on-the-fly.
     * Optional flags can be passed through to underlying the location. 
     * @since 0.7.0, but beta and likely to change as the semantics of this class are tuned 
     * @deprecated since 0.9.0 use {@link #getLocationSpec(String, Map)} or {@link #getLocationManaged(String, Map)} */
    @SuppressWarnings("rawtypes")
    @Deprecated
    public Maybe<Location> resolve(String spec, Boolean manage, Map locationFlags);

    /** See {@link #resolve(String, Boolean, Map)}; asks for the location to be managed, and supplies no additional flags,
     * and unwraps the result (throwing if the spec cannot be resolve).
     * @deprecated since 0.9.0 use {@link #getLocationSpec(String)} or {@link #getLocationManaged(String)} */
    @Deprecated
    public Location resolve(String spec);
        
    /** Returns true/false depending whether spec seems like a valid location,
     * that is it has a chance of being resolved (depending on the spec) but NOT guaranteed,
     * as it is not passed to the spec;
     * see {@link #resolve(String, Boolean, Map)} which has stronger guarantees 
     * @deprecated since 0.7.0, not really needed, and semantics are weak; use {@link #resolve(String, Boolean, Map)} */
    @Deprecated
    public boolean canMaybeResolve(String spec);
    
    /** As {@link #resolve(String, Boolean, Map)}, but unwrapped
     * @throws NoSuchElementException if the spec cannot be resolved 
     * @deprecated since 0.9.0 use {@link #getLocationSpec(String, Map)} and then manage it as needed*/
    @SuppressWarnings("rawtypes")
    @Deprecated
    public Location resolve(String spec, @Nullable Map locationFlags);
    
    /**
     * As {@link #getLocationManaged(String)} but takes collections (of strings or locations)
     * <p>
     * Expects a collection of elements being individual location spec strings or locations, 
     * and returns a list of resolved (newly created and managed) locations.
     * <p>
     * From 0.7.0 this no longer flattens lists (nested lists are disallowed) 
     * or parses comma-separated elements (they are resolved as-is)
     * @deprecated since 0.9.0 use {@link #getListOfLocationsManaged(Object)} */
    @Deprecated
    public List<Location> resolve(Iterable<?> spec);
    
    /** Takes a string, interpreted as a comma-separated (or JSON style, when you need internal double quotes or commas) list;
     * or a list, passed to {@link #resolve(Iterable)}; or null/empty (empty list),
     * and returns a list of resolved (created and managed) locations 
     * @deprecated since 0.9.0 use {@link #getListOfLocationsManaged(Object)} */
    @Deprecated
    public List<Location> resolveList(Object specList);

    /** Create a {@link LocationSpec} representing the given spec string such as a named location 
     * or using a resolver prefix such as jclouds:aws-ec2. 
     * This can then be inspected, assigned to an {@link EntitySpec}, 
     * or passed to {@link LocationManager#createLocation(LocationSpec)} to create directly.
     * (For that last case, common in tests, see {@link #getLocationManaged(String)}.) */
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(String spec);
    /** As {@link #getLocationSpec(String)} but also setting the given flags configured on the resulting spec. */
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(String spec, Map<?,?> locationFlags);

    /** As {@link #getLocationSpec(String)} where the caller has a {@link LocationDefinition}. */
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(LocationDefinition ld);
    /** As {@link #getLocationSpec(String,Map)} where the caller has a {@link LocationDefinition}. */
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(LocationDefinition ld, Map<?,?> locationFlags);
    
    /** A combination of {@link #getLocationSpec(String)} then {@link LocationManager#createLocation(LocationSpec)},
     * mainly for use in tests or specialised situations where a managed location is needed directly.
     * The caller is responsible for ensuring that the resulting {@link Location} 
     * is cleaned up, ie removed from management via {@link LocationManager#unmanage(Location)} directly or linking it to 
     * an {@link Entity} or another {@link Location} which will unmanage it. */
    public Location getLocationManaged(String spec);
    /** As {@link #getLocationManaged(String)} applying the config as per {@link #getLocationSpec(String, Map)}. */
    public Location getLocationManaged(String spec, Map<?,?> locationFlags);
    
    /** Takes a string, interpreted as a comma-separated (or JSON style, when you need internal double quotes or commas) list;
     * or a list of strings, or null (giving the empty list) and returns a list of managed locations.
     * Note that lists of lists are not permitted.
     * <p>
     * The caller is responsible for ensuring these get cleaned up, as described at {@link #getLocationManaged(String)}. */
    public List<Location> getListOfLocationsManaged(Object specList);

    @SuppressWarnings("rawtypes")
    public Map getProperties();
    
}
