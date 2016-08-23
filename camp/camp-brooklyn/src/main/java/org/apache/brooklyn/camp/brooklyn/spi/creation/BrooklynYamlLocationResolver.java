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
package org.apache.brooklyn.camp.brooklyn.spi.creation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.Maybe.Absent;
import org.apache.brooklyn.util.text.Strings;

public class BrooklynYamlLocationResolver {

    protected final ManagementContext mgmt;

    public BrooklynYamlLocationResolver(ManagementContext bmc) {
        this.mgmt = bmc;
    }

    /** returns list of locations, if any were supplied, or null if none indicated */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<LocationSpec<?>> resolveLocations(Map<? super String,?> attrs, boolean removeUsedAttributes) {
        Object location = attrs.get("location");
        Object locations = attrs.get("locations");

        if (location==null && locations==null)
            return null;

        LocationSpec<?> locationFromString = null;
        List<LocationSpec<?>> locationsFromList = null;

        if (location!=null) {
            if (location instanceof String) {
                locationFromString = resolveLocationFromString((String) location);
            } else if (location instanceof Map) {
                locationFromString = resolveLocationFromMap((Map<?,?>) location);
            } else {
                throw new UserFacingException("Illegal parameter for 'location'; must be a string or map (but got "+location+")");
            }
        }

        if (locations!=null) {
            if (!(locations instanceof Iterable))
                throw new UserFacingException("Illegal parameter for 'locations'; must be an iterable (but got "+locations+")");
            locationsFromList = resolveLocations((Iterable<Object>) locations );
        }

        if (locationFromString != null && locationsFromList != null) {
            if (locationsFromList.size() != 1)
                throw new UserFacingException("Conflicting 'location' and 'locations' ("+location+" and "+locations+"); "
                    + "if both are supplied the list must have exactly one element being the same");
            if (!locationFromString.equals(Iterables.getOnlyElement(locationsFromList)))
                throw new UserFacingException("Conflicting 'location' and 'locations' ("+location+" and "+locations+"); "
                    + "different location specified in each");
        } else if (locationFromString != null) {
            locationsFromList = (List) Arrays.asList(locationFromString);
        }

        return locationsFromList;
    }

    public List<LocationSpec<?>> resolveLocations(Iterable<Object> locations) {
        List<LocationSpec<?>> result = MutableList.of();
        for (Object l : locations) {
            LocationSpec<?> ll = resolveLocation(l);
            if (ll!=null) result.add(ll);
        }
        return result;
    }

    public LocationSpec<?> resolveLocation(Object location) {
        if (location instanceof String) {
            return resolveLocationFromString((String)location);
        } else if (location instanceof Map) {
            return resolveLocationFromMap((Map<?,?>)location);
        }
        // could support e.g. location definition
        throw new UserFacingException("Illegal parameter for 'location' ("+location+"); must be a string or map");
    }

    /**
     * Resolves the location from the given spec string.
     * <p>
     * Either {@code Named Location}, or {@code named:Named Location} format.
     *
     * @return null if input is blank (or null) otherwise guaranteed to resolve or throw error
     */
    public LocationSpec<?> resolveLocationFromString(String location) {
        if (Strings.isBlank(location)) return null;
        return resolveLocation(location, MutableMap.of());
    }

    public LocationSpec<?> resolveLocationFromMap(Map<?,?> location) {
        if (location.size() > 1) {
            throw new UserFacingException("Illegal parameter for 'location'; expected a single entry in map ("+location+")");
        }
        Object key = Iterables.getOnlyElement(location.keySet());
        Object value = location.get(key);

        if (!(key instanceof String)) {
            throw new UserFacingException("Illegal parameter for 'location'; expected String key ("+location+")");
        }
        if (!(value instanceof Map)) {
            throw new UserFacingException("Illegal parameter for 'location'; expected config map ("+location+")");
        }
        return resolveLocation((String) key, (Map<?,?>) value);
    }

    protected LocationSpec<?> resolveLocation(String spec, Map<?,?> flags) {
        LocationDefinition ldef = mgmt.getLocationRegistry().getDefinedLocationByName(spec);
        if (ldef != null)
            // found it as a named location
            return mgmt.getLocationRegistry().getLocationSpec(ldef, flags).get();

        Maybe<LocationSpec<?>> l = mgmt.getLocationRegistry().getLocationSpec(spec, flags);
        if (l.isPresent()) return l.get();

        RuntimeException exception = ((Absent<?>)l).getException();
        if (exception instanceof NoSuchElementException && exception.getMessage().contains("Unknown location")) {
            throw new UserFacingException(exception.getMessage(), exception.getCause());
        }
        throw new UserFacingException("Illegal parameter for 'location' ("+spec+"); not resolvable: " + Exceptions.collapseText(exception), exception);
    }
}
