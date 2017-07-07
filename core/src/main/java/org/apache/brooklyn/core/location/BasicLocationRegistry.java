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
package org.apache.brooklyn.core.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.framework.FrameworkLookup;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.internal.LocalLocationManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.location.localhost.LocalhostLocationResolver;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.apache.brooklyn.util.text.WildcardGlobs.PhraseTreatment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

/**
 * See {@link LocationRegistry} for general description.
 * <p>
 * This implementation unfortunately has to deal with location definitions coming from three places:
 * 
 * <li> brooklyn.properties
 * <li> {@link BrooklynCatalog}
 * <li> {@link BrooklynTypeRegistry}
 * <p> 
 * The first is read on init in the call to {@link #updateDefinedLocations()}.
 * The latter two are <i>not</i> loaded here but are treated as lookups to {@link BrooklynTypeRegistry}.
 * <p>
 * (Prior to 0.12.0 items in {@link BrooklynCatalog} were scanned on init here, and then 
 * it updated this on additions and removals there. That is no longer done with type registry.)
 * <p>
 * We should consider deprecating this as a place where locations can be defined (switch to type registry only).
 * <p>
 * However this is used for items from brooklyn.properties -- we could create a bundle to store those
 * to help with migration, and then deprecate/block changes to those after the bundle is created. 
 * However properties are the only way currently to set default data used for various clouds; 
 * either we'd need to deprecate that also, or find a way to support that in a bundled world.
 * <p>
 * This is also used in a few other places, such as clocker and the server pool entity.
 * Things here are never persisted though, so it is those callers' responsibility to recreate.
 * Alternatively (better) would be for them to add items to the catalog instead of here. 
 * <p>
 * Or we keep this lying around for now. Probably not worth the pain above.
 * <p>
 * Also note the location item in the catalog has an unparsed blob of YAML, which contains
 * important things like the type and the config of the location. This is only parsed when 
 * {@link BrooklynTypeRegistry#createSpec(RegisteredType)} is called, so we do that for
 * lookup possibly more often than one might expect. (If a time hole this could be cached.)
 * We also need to make sure some of the resolvers here can be used to identify types, so
 * when loading types, the spec creation routines are able to call this to 
 * {@link #resolve(String, Boolean, Map)} (that returns a {@link Location} not a spec,
 * but the {@link Location} instance is discarded).
 * <p>
 * Finally note /v1/catalog/locations does not list items stored here; 
 * only /v1/locations does that.
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class BasicLocationRegistry implements LocationRegistry {

    public static final Logger log = LoggerFactory.getLogger(BasicLocationRegistry.class);

    /**
     * Splits a comma-separated list of locations (names or specs) into an explicit list.
     * The splitting is very careful to handle commas embedded within specs, to split correctly.
     */
    public static List<String> expandCommaSeparateLocations(String locations) {
        return WildcardGlobs.getGlobsAfterBraceExpansion("{"+locations+"}", false, PhraseTreatment.INTERIOR_NOT_EXPANDABLE, PhraseTreatment.INTERIOR_NOT_EXPANDABLE);
        // don't do this, it tries to expand commas inside parentheses which is not good!
//        QuotedStringTokenizer.builder().addDelimiterChars(",").buildList((String)id);
    }

    private final ManagementContext mgmt;
    /** map of defined locations by their ID */
    private final Map<String,LocationDefinition> definedLocations = new LinkedHashMap<String, LocationDefinition>();

    protected final Map<String,LocationResolver> resolvers = new LinkedHashMap<String, LocationResolver>();

    private final Set<String> specsWarnedOnException = Sets.newConcurrentHashSet();

    public BasicLocationRegistry(ManagementContext mgmt) {
        this.mgmt = checkNotNull(mgmt, "mgmt");
        findServices();
        updateDefinedLocations();
    }

    protected void findServices() {
        Iterable<LocationResolver> loader = FrameworkLookup.lookupAll(LocationResolver.class, mgmt.getCatalogClassLoader());
        MutableList<LocationResolver> loadedResolvers;
        try {
            loadedResolvers = MutableList.copyOf(loader);
        } catch (Throwable e) {
            log.warn("Error loading resolvers (rethrowing): "+e);
            throw Exceptions.propagate(e);
        }
        
        for (LocationResolver r: loadedResolvers) {
            registerResolver(r);
        }
        if (log.isDebugEnabled()) log.debug("Location resolvers are: "+resolvers);
        if (resolvers.isEmpty()) log.warn("No location resolvers detected: is src/main/resources correctly included?");
    }

    /** Registers the given resolver, invoking {@link LocationResolver#init(ManagementContext)} on the argument
     * and returning true, unless the argument indicates false for {@link LocationResolver.EnableableLocationResolver#isEnabled()} */
    public boolean registerResolver(LocationResolver r) {
        r.init(mgmt);
        if (!r.isEnabled()) {
            return false;
        }
        resolvers.put(r.getPrefix(), r);
        return true;
    }
    
    @Override
    public Map<String, LocationDefinition> getDefinedLocations(boolean includeThingsWeAreFacadeFor) {
        Map<String, LocationDefinition> result = MutableMap.of();
        synchronized (definedLocations) {
            result.putAll(definedLocations);
        }
        for (RegisteredType rt: mgmt.getTypeRegistry().getMatching(RegisteredTypePredicates.IS_LOCATION)) {
            result.put(rt.getId(), newDefinedLocation(rt));
        }
        return result;
    }
    
    @Override @Deprecated
    public Map<String,LocationDefinition> getDefinedLocations() {
        return getDefinedLocations(true);
    }
    
    @Override
    public LocationDefinition getDefinedLocationById(String id) {
        return getDefinedLocation(id, true);
    }

    @Override
    public LocationDefinition getDefinedLocationByName(String name) {
        return getDefinedLocation(name, false);
    }
    
    private LocationDefinition getDefinedLocation(String nameOrId, boolean isId) {
        RegisteredType lt = mgmt.getTypeRegistry().get(nameOrId, RegisteredTypeLoadingContexts.withSpecSuperType(null, LocationSpec.class));
        if (lt!=null) {
            return newDefinedLocation(lt);
        }
        
        synchronized (definedLocations) {
            if (isId) {
                LocationDefinition ld = definedLocations.get(nameOrId);
                if (ld!=null) {
                    return ld;
                }
            } else {
                for (LocationDefinition l: definedLocations.values()) {
                    if (l.getName().equals(nameOrId)) return l;
                }
            }
        }
        
        // fall back to ignoring supertypes, in case they weren't set
        lt = mgmt.getTypeRegistry().get(nameOrId);
        if (lt!=null && lt.getSuperTypes().isEmpty()) {
            if (lt.getKind()!=RegisteredTypeKind.UNRESOLVED) {
                // don't warn when still trying to resolve
                log.warn("Location registry only found "+nameOrId+" when ignoring supertypes; check it is correctly resolved.");
            }
            return newDefinedLocation(lt);
        }
        
        return null;
    }

    @Override @Deprecated
    public void updateDefinedLocation(LocationDefinition l) {
        updateDefinedLocationNonPersisted(l);
    }
    
    @Override
    public void updateDefinedLocationNonPersisted(LocationDefinition l) {
        synchronized (definedLocations) { 
            definedLocations.put(l.getId(), l); 
        }
    }

    /**
     * Converts the given item from the catalog into a LocationDefinition, and adds it
     * to the registry (overwriting anything already registered with the id
     * {@link CatalogItem#getCatalogItemId()}.
     * @deprecated since 0.12.0 this class is a facade so method no longer wanted
     */
    @Deprecated  // not used
    public void updateDefinedLocation(CatalogItem<Location, LocationSpec<?>> item) {
        String id = item.getCatalogItemId();
        String symbolicName = item.getSymbolicName();
        String spec = CatalogLocationResolver.createLegacyWrappedReference(id);
        Map<String, Object> config = ImmutableMap.<String, Object>of();
        BasicLocationDefinition locDefinition = new BasicLocationDefinition(symbolicName, symbolicName, spec, config);
        
        updateDefinedLocation(locDefinition);
    }

    /**
     * Converts the given item from the catalog into a LocationDefinition, and adds it
     * to the registry (overwriting anything already registered with the id
     * {@link RegisteredType#getId()}.
     * @deprecated since 0.12.0 this class is a facade so method no longer wanted
     */
    @Deprecated  // not used
    public void updateDefinedLocation(RegisteredType item) {
        BasicLocationDefinition locDefinition = newDefinedLocation(item);
        updateDefinedLocation(locDefinition);
    }

    protected BasicLocationDefinition newDefinedLocation(RegisteredType item) {
        String id = item.getId();
        String spec = CatalogLocationResolver.createLegacyWrappedReference(id);
        return new BasicLocationDefinition(id, item.getSymbolicName(), spec, ImmutableMap.<String, Object>of());
    }

    /** @deprecated since 0.12.0 this class is a facade so method no longer wanted */
    @Deprecated  // not used
    public void removeDefinedLocation(CatalogItem<Location, LocationSpec<?>> item) {
        removeDefinedLocation(item.getSymbolicName());
    }
    
    @Override
    public void removeDefinedLocation(String id) {
        LocationDefinition removed;
        synchronized (definedLocations) { 
            removed = definedLocations.remove(id);
        }
        if (removed == null && log.isDebugEnabled()) {
            log.debug("{} was asked to remove location with id {} but no such location was registered", this, id);
        }
    }
    
    public void updateDefinedLocations() {
        synchronized (definedLocations) {
            // first read all properties starting  brooklyn.location.named.xxx
            // (would be nice to move to a better way, e.g. yaml, then deprecate this approach, but first
            // we need ability/format for persisting named locations, and better support for adding+saving via REST/GUI)
            int count = 0; 
            String NAMED_LOCATION_PREFIX = "brooklyn.location.named.";
            StringConfigMap namedLocationProps = mgmt.getConfig().submap(ConfigPredicates.nameStartsWith(NAMED_LOCATION_PREFIX));
            for (String k: namedLocationProps.asMapWithStringKeys().keySet()) {
                String name = k.substring(NAMED_LOCATION_PREFIX.length());
                // If has a dot, then is a sub-property of a named location (e.g. brooklyn.location.named.prod1.user=bob)
                if (!name.contains(".")) {
                    // this is a new named location
                    String spec = (String) namedLocationProps.asMapWithStringKeys().get(k);
                    // make up an ID
                    String id = Identifiers.makeRandomId(8);
                    BrooklynProperties config = ConfigUtils.filterForPrefixAndStrip(namedLocationProps.asMapWithStringKeys(), k+".");
                    definedLocations.put(id, new BasicLocationDefinition(id, name, spec, config.asMapWithStringKeys()));
                    count++;
                }
            }
            if (log.isDebugEnabled())
                log.debug("Found "+count+" defined locations from properties (*.named.* syntax): "+definedLocations.values());
        }
    }
    
    private static BasicLocationDefinition localhost(String id) {
        return new BasicLocationDefinition(id, "localhost", "localhost", null);
    }
    
    /** to catch circular references */
    protected ThreadLocal<Set<String>> specsSeen = new ThreadLocal<Set<String>>();
    
    @Override @Deprecated
    public boolean canMaybeResolve(String spec) {
        return getSpecResolver(spec) != null;
    }

    @Override @Deprecated
    public final Location resolve(String spec) {
        return resolve(spec, true, null).get();
    }
    
    @Override @Deprecated
    public Maybe<Location> resolve(String spec, Boolean manage, Map locationFlags) {
        if (manage!=null) {
            locationFlags = MutableMap.copyOf(locationFlags);
            locationFlags.put(LocalLocationManager.CREATE_UNMANAGED, !manage);
        }
        Maybe<LocationSpec<? extends Location>> lSpec = getLocationSpec(spec, locationFlags);
        if (lSpec.isAbsent()) return (Maybe)lSpec;
        return Maybe.of((Location)mgmt.getLocationManager().createLocation(lSpec.get()));
    }

    @Override
    public Location getLocationManaged(String spec) {
        return mgmt.getLocationManager().createLocation(getLocationSpec(spec).get());
    }
    
    @Override
    public final Location getLocationManaged(String spec, Map locationFlags) {
        return mgmt.getLocationManager().createLocation(getLocationSpec(spec, locationFlags).get());
    }

    @Override
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(LocationDefinition ld) {
        return getLocationSpec(ld, MutableMap.of());
    }
    
    @Override
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(LocationDefinition ld, Map locationFlags) {
        ConfigBag newLocationFlags = ConfigBag.newInstance(ld.getConfig())
            .putAll(locationFlags)
            .putIfAbsentAndNotNull(LocationInternal.NAMED_SPEC_NAME, ld.getName())
            .putIfAbsentAndNotNull(LocationInternal.ORIGINAL_SPEC, ld.getName());
        Maybe<LocationSpec<? extends Location>> result = getLocationSpec(ld.getSpec(), newLocationFlags.getAllConfigRaw());
        if (result.isPresent()) {
            result.get().configure(LocationInternal.NAMED_SPEC_NAME, ld.getName());
            result.get().configure(LocationInternal.ORIGINAL_SPEC, ld.getName());
        }
        return result;
    }

    @Override
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(String spec) {
        return getLocationSpec(spec, MutableMap.of());
    }
    @Override
    public Maybe<LocationSpec<? extends Location>> getLocationSpec(String spec, Map locationFlags) {
        try {
            locationFlags = MutableMap.copyOf(locationFlags);
            
            Set<String> seenSoFar = specsSeen.get();
            if (seenSoFar==null) {
                seenSoFar = new LinkedHashSet<String>();
                specsSeen.set(seenSoFar);
            }
            if (seenSoFar.contains(spec))
                return Maybe.absent(Suppliers.ofInstance(new IllegalStateException("Circular reference in definition of location '"+spec+"' ("+seenSoFar+")")));
            seenSoFar.add(spec);
            
            LocationResolver resolver = getSpecResolver(spec);

            if (resolver != null) {
                try {
                    LocationSpec<? extends Location> specO = resolver.newLocationSpecFromString(spec, locationFlags, this);
                    specO.configure(LocationInternal.ORIGINAL_SPEC, spec);
                    specO.configure(LocationInternal.NAMED_SPEC_NAME, spec);
                    return (Maybe) Maybe.of(specO);
                } catch (RuntimeException e) {
                     return Maybe.absent(Suppliers.ofInstance(e));
                }
            }

            // problem: but let's ensure that classpath is sane to give better errors in common IDE bogus case;
            // and avoid repeated logging
            String errmsg = "Unknown location '"+spec+"'";
            ConfigBag cfg = ConfigBag.newInstance(locationFlags);
            String orig = cfg.get(LocationInternal.ORIGINAL_SPEC);                
            String named = cfg.get(LocationInternal.NAMED_SPEC_NAME);
            if (Strings.isNonBlank(named) && !named.equals(spec) && !named.equals(orig)) {
                errmsg += " when looking up '"+named+"'";
            }
            if (Strings.isNonBlank(orig) && !orig.equals(spec)) {
                errmsg += " when resolving '"+orig+"'";
            }

            if (spec == null || specsWarnedOnException.add(spec)) {
                if (resolvers.get("id")==null || resolvers.get("named")==null) {
                    log.error("Standard location resolvers not installed, location resolution will fail shortly. "
                            + "This usually indicates a classpath problem, such as when running from an IDE which "
                            + "has not properly copied META-INF/services from src/main/resources. "
                            + errmsg+". "
                            + "Known resolvers are: "+resolvers.keySet());
                    errmsg = errmsg+": "
                            + "Problem detected with location resolver configuration; "
                            + resolvers.keySet()+" are the only available location resolvers. "
                            + "More information can be found in the logs.";
                } else {
                    if (log.isDebugEnabled()) log.debug(errmsg + " (if this is being loaded it will fail shortly): known resolvers are: "+resolvers.keySet());
                }
            } else {
                if (log.isDebugEnabled()) log.debug(errmsg + "(with retry, already warned)");
                errmsg += " (with retry)";
            }

            return Maybe.absent(Suppliers.ofInstance(new NoSuchElementException(errmsg)));

        } finally {
            specsSeen.remove();
        }
    }

    @Override @Deprecated
    public final Location resolve(String spec, Map locationFlags) {
        return resolve(spec, null, locationFlags).get();
    }
    
    protected LocationResolver getSpecResolver(String spec) {
        int colonIndex = spec.indexOf(':');
        int bracketIndex = spec.indexOf("(");
        int dividerIndex = (colonIndex < 0) ? bracketIndex : (bracketIndex < 0 ? colonIndex : Math.min(bracketIndex, colonIndex));
        String prefix = dividerIndex >= 0 ? spec.substring(0, dividerIndex) : spec;
        LocationResolver resolver = resolvers.get(prefix);
       
        if (resolver == null)
            resolver = getSpecDefaultResolver(spec);
        
        return resolver;
    }
    
    protected LocationResolver getSpecDefaultResolver(String spec) {
        return getSpecFirstResolver(spec, "id", "named", "jclouds");
    }
    protected LocationResolver getSpecFirstResolver(String spec, String ...resolversToCheck) {
        for (String resolverId: resolversToCheck) {
            LocationResolver resolver = resolvers.get(resolverId);
            if (resolver!=null && resolver.accepts(spec, this))
                return resolver;
        }
        return null;
    }

    /** providers default impl for {@link LocationResolver#accepts(String, LocationRegistry)} */
    public static boolean isResolverPrefixForSpec(LocationResolver resolver, String spec, boolean argumentRequired) {
        if (spec==null) return false;
        if (spec.startsWith(resolver.getPrefix()+":")) return true;
        if (!argumentRequired && spec.equals(resolver.getPrefix())) return true;
        return false;
    }

    @Override @Deprecated
    public List<Location> resolve(Iterable<?> spec) {
        return getFromIterableListOfLocationsManaged(spec);
    }
    
    private List<Location> getFromIterableListOfLocationsManaged(Iterable<?> spec) {
        List<Location> result = new ArrayList<Location>();
        for (Object id : spec) {
            if (id==null) {
                // drop a null entry
            } if (id instanceof String) {
                result.add(getLocationManaged((String) id));
            } else if (id instanceof Location) {
                result.add((Location) id);
            } else {
                if (id instanceof Iterable)
                    throw new IllegalArgumentException("Cannot resolve '"+id+"' to a location; collections of collections not allowed"); 
                throw new IllegalArgumentException("Cannot resolve '"+id+"' to a location; unsupported type "+
                        (id == null ? "null" : id.getClass().getName())); 
            }
        }
        return result;
    }
    
    @Override @Deprecated
    public List<Location> resolveList(Object l) {
        return getListOfLocationsManaged(l);
    }
    
    @Override 
    public List<Location> getListOfLocationsManaged(Object l) {
        if (l==null) l = Collections.emptyList();
        if (l instanceof String) l = JavaStringEscapes.unwrapJsonishListIfPossible((String)l);
        if (l instanceof Iterable) return getFromIterableListOfLocationsManaged((Iterable<?>)l);
        throw new IllegalArgumentException("Location list must be supplied as a collection or a string, not "+
            JavaClassNames.simpleClassName(l)+"/"+l);
    }
    
    @Override @Deprecated
    public Location resolve(LocationDefinition ld) {
        return resolve(ld, null, null).get();
    }
    
    @Override @Deprecated
    public Maybe<Location> resolve(LocationDefinition ld, Boolean manage, Map locationFlags) {
        ConfigBag newLocationFlags = ConfigBag.newInstance(ld.getConfig())
            .putAll(locationFlags)
            .putIfAbsentAndNotNull(LocationInternal.NAMED_SPEC_NAME, ld.getName())
            .putIfAbsentAndNotNull(LocationInternal.ORIGINAL_SPEC, ld.getName());
        Maybe<Location> result = resolve(ld.getSpec(), manage, newLocationFlags.getAllConfigRaw());
        if (result.isPresent()) 
            return result;
        throw new IllegalStateException("Cannot instantiate location '"+ld+"' pointing at "+ld.getSpec(), 
            Maybe.getException(result) );
    }
    
    @Override
    public Map getProperties() {
        return mgmt.getConfig().asMapWithStringKeys();
    }

    @VisibleForTesting
    public void putProperties(Map<String, ?> vals) {
        ((ManagementContextInternal)mgmt).getBrooklynProperties().putAll(vals);
    }

    @VisibleForTesting
    public static void addNamedLocationLocalhost(ManagementContext mgmt) {
        if (!mgmt.getConfig().getConfig(LocalhostLocationResolver.LOCALHOST_ENABLED)) {
            throw new IllegalStateException("Localhost is disabled.");
        }
        
        // ensure localhost is added (even on windows, it's just for testing)
        LocationDefinition l = mgmt.getLocationRegistry().getDefinedLocationByName("localhost");
        if (l==null) mgmt.getLocationRegistry().updateDefinedLocation(
                BasicLocationRegistry.localhost(Identifiers.makeRandomId(8)) );
    }
    
}
