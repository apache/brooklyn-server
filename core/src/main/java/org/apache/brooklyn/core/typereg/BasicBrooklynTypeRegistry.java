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

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogItemBuilder;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypes.RegisteredTypeNameThenBestFirstComparator;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.concurrent.Locks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.osgi.VersionedName.VersionedNameComparator;
import org.apache.brooklyn.util.osgi.VersionedName.VersionedNameStringComparator;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

public class BasicBrooklynTypeRegistry implements BrooklynTypeRegistry {

    private static final Logger log = LoggerFactory.getLogger(BasicBrooklynTypeRegistry.class);
    
    private ManagementContext mgmt;
    private Map<String,Map<String,RegisteredType>> localRegisteredTypesAndContainingBundles = MutableMap.of();
    /**
     * Thread synch model is pretty simple, as follows:
     * - get a read lock on this if looking at localRegisteredTypesAndContainingBundles
     *   or any of the maps contained within;
     * - get a write lock on this if changing the map above or any of the maps within.
     * 
     * There is potential for finer grained locking to allow reads/writes of different inner
     * maps but coordinating that is tricky and does not seem worth it.
     */
    private ReadWriteLock localRegistryLock = new ReentrantReadWriteLock();

    public BasicBrooklynTypeRegistry(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }
    
    @Override
    public Iterable<RegisteredType> getAll() {
        return getMatching(Predicates.alwaysTrue());
    }
    
    private Iterable<RegisteredType> getAllWithoutCatalog(Predicate<? super RegisteredType> filter) {
        // TODO optimisation? make indexes and look up?
        Ordering<RegisteredType> typeOrder = Ordering.from(RegisteredTypeNameThenBestFirstComparator.INSTANCE);
        return Locks.withLock(localRegistryLock.readLock(), 
            () -> localRegisteredTypesAndContainingBundles.values().stream().
                flatMap(m -> { return typeOrder.sortedCopy(m.values()).stream(); }).filter(filter::apply).collect(Collectors.toList()) );
    }

    private Maybe<RegisteredType> getExactWithoutLegacyCatalog(String symbolicName, String version, RegisteredTypeLoadingContext constraint) {
        RegisteredType item = Locks.withLock(localRegistryLock.readLock(), 
            ()-> getBestValue(localRegisteredTypesAndContainingBundles.get(symbolicName+":"+version)) );
        return RegisteredTypes.tryValidate(item, constraint);
    }

    private RegisteredType getBestValue(Map<String, RegisteredType> m) {
        if (m==null) return null;
        if (m.isEmpty()) return null;
        if (m.size()==1) return m.values().iterator().next();
        // get the highest version of first alphabetical - to have a canonical order
        return m.get( Ordering.from(VersionedNameStringComparator.INSTANCE).min(m.keySet()) );
    }

    @SuppressWarnings("deprecation")
    @Override
    public Iterable<RegisteredType> getMatching(Predicate<? super RegisteredType> filter) {
        Set<RegisteredType> result = MutableSet.of();
        // keep name record also so we can remove legacy items that are superseded
        Set<String> typeNamesFound = MutableSet.of();
        for (RegisteredType rt: getAllWithoutCatalog(filter)) {
            result.add(rt);
            typeNamesFound.add(rt.getId());
        }
        for (RegisteredType rt: Iterables.filter(
                Iterables.transform(mgmt.getCatalog().getCatalogItemsLegacy(), RegisteredTypes.CI_TO_RT), 
                filter)) {
            if (!typeNamesFound.contains(rt.getId())) {
                // TODO ideally never come here, however
                // legacy cataog currently still used for java-scanned annotations; 
                // hopefully that will be deprecated and removed in near future
                // (probably after switch to osgi and using catalog.bom --
                // though it would not be too hard for java scan code in CatalogClasspath.load to
                // make TypeRegistry instances instead of CatalogItem, esp if we had YOML to write that plan)
                
                //log.warn("Item '"+rt.getId()+"' not in type registry; only found in legacy catalog");
                typeNamesFound.add(rt.getId());
                result.add(rt);
            }
        }
        return result;
    }

    @SuppressWarnings("deprecation")
    private Maybe<RegisteredType> getSingle(String symbolicNameOrAliasIfNoVersion, final String versionFinal, final RegisteredTypeLoadingContext contextFinal) {
        RegisteredTypeLoadingContext context = contextFinal;
        if (context==null) context = RegisteredTypeLoadingContexts.any();
        String version = versionFinal;
        if (Strings.isBlank(version)) version = BrooklynCatalog.DEFAULT_VERSION;

        if (!BrooklynCatalog.DEFAULT_VERSION.equals(version)) {
            // normal code path when version is supplied
            
            Maybe<RegisteredType> type = getExactWithoutLegacyCatalog(symbolicNameOrAliasIfNoVersion, version, context);
            if (type.isPresent()) return type;
        }

        if (BrooklynCatalog.DEFAULT_VERSION.equals(version)) {
            // alternate code path, if version blank or default
            
            Iterable<RegisteredType> types = getMatching(Predicates.and(RegisteredTypePredicates.symbolicName(symbolicNameOrAliasIfNoVersion), 
                RegisteredTypePredicates.satisfies(context)));
            if (Iterables.isEmpty(types)) {
                // look for alias if no exact symbolic name match AND no version is specified
                types = getMatching(Predicates.and(RegisteredTypePredicates.alias(symbolicNameOrAliasIfNoVersion), 
                    RegisteredTypePredicates.satisfies(context) ) );
                // if there are multiple symbolic names then throw?
                Set<String> uniqueSymbolicNames = MutableSet.of();
                for (RegisteredType t: types) {
                    uniqueSymbolicNames.add(t.getSymbolicName());
                }
                if (uniqueSymbolicNames.size()>1) {
                    String message = "Multiple matches found for alias '"+symbolicNameOrAliasIfNoVersion+"': "+uniqueSymbolicNames+"; "
                        + "refusing to select any.";
                    log.warn(message);
                    return Maybe.absent(message);
                }
            }
            if (!Iterables.isEmpty(types)) {
                RegisteredType type = RegisteredTypes.getBestVersion(types);
                if (type!=null) return Maybe.of(type);
            }
        }
        
        // missing case is to look for exact version in legacy catalog
        CatalogItem<?, ?> item = mgmt.getCatalog().getCatalogItemLegacy(symbolicNameOrAliasIfNoVersion, version);
        if (item!=null) 
            return Maybe.of( RegisteredTypes.CI_TO_RT.apply( item ) );
        
        return Maybe.absent("No matches for "+symbolicNameOrAliasIfNoVersion+
            (Strings.isNonBlank(versionFinal) ? ":"+versionFinal : "")+
            (contextFinal!=null ? " ("+contextFinal+")" : "") );
    }

    @Override
    public RegisteredType get(String symbolicName, String version) {
        return getSingle(symbolicName, version, null).orNull();
    }
    
    @Override
    public RegisteredType get(String symbolicNameWithOptionalVersion, RegisteredTypeLoadingContext context) {
        return getMaybe(symbolicNameWithOptionalVersion, context).orNull();
    }
    @Override
    public Maybe<RegisteredType> getMaybe(String symbolicNameWithOptionalVersion, RegisteredTypeLoadingContext context) {
        Maybe<RegisteredType> r1 = null;
        if (RegisteredTypeNaming.isUsableTypeColonVersion(symbolicNameWithOptionalVersion) ||
                // included through 0.12 so legacy type names are accepted (with warning)
                CatalogUtils.looksLikeVersionedId(symbolicNameWithOptionalVersion)) {
            String symbolicName = CatalogUtils.getSymbolicNameFromVersionedId(symbolicNameWithOptionalVersion);
            String version = CatalogUtils.getVersionFromVersionedId(symbolicNameWithOptionalVersion);
            r1 = getSingle(symbolicName, version, context);
            if (r1.isPresent()) return r1;
        }

        Maybe<RegisteredType> r2 = getSingle(symbolicNameWithOptionalVersion, BrooklynCatalog.DEFAULT_VERSION, context);
        if (r2.isPresent() || r1==null) return r2;
        return r1;
    }

    @Override
    public RegisteredType get(String symbolicNameWithOptionalVersion) {
        return get(symbolicNameWithOptionalVersion, (RegisteredTypeLoadingContext)null);
    }

    @Override
    public <SpecT extends AbstractBrooklynObjectSpec<?,?>> SpecT createSpec(RegisteredType type, @Nullable RegisteredTypeLoadingContext constraint, @Nullable Class<SpecT> specSuperType) {
        Preconditions.checkNotNull(type, "type");
        if (type.getKind()==RegisteredTypeKind.SPEC) {
            return createSpec(type, type.getPlan(), type.getSymbolicName(), type.getVersion(), type.getSuperTypes(), constraint, specSuperType);
            
        } else if (type.getKind()==RegisteredTypeKind.UNRESOLVED) {
            if (constraint != null && constraint.getAlreadyEncounteredTypes().contains(type.getSymbolicName())) {
                throw new UnsupportedTypePlanException("Cannot create spec from type "+type+" (kind "+type.getKind()+"), recursive reference following "+constraint.getAlreadyEncounteredTypes());
                
            } else {
                // try just-in-time validation
                Collection<Throwable> validationErrors = mgmt.getCatalog().validateType(type, constraint);
                if (!validationErrors.isEmpty()) {
                    throw new ReferencedUnresolvedTypeException(type, true, Exceptions.create(validationErrors));
                }
                type = mgmt.getTypeRegistry().get(type.getSymbolicName(), type.getVersion());
                if (type==null || type.getKind()==RegisteredTypeKind.UNRESOLVED) {
                    throw new ReferencedUnresolvedTypeException(type);
                }
                return createSpec(type, constraint, specSuperType);
            }
            
        } else {
            throw new UnsupportedTypePlanException("Cannot create spec from type "+type+" (kind "+type.getKind()+")");
        }
    }
    
    @SuppressWarnings({ "deprecation", "unchecked", "rawtypes" })
    private <SpecT extends AbstractBrooklynObjectSpec<?,?>> SpecT createSpec(
            RegisteredType type,
            TypeImplementationPlan plan,
            @Nullable String symbolicName, @Nullable String version, Set<Object> superTypes,
            @Nullable RegisteredTypeLoadingContext constraint, @Nullable Class<SpecT> specSuperType) {
        // TODO type is only used to call to "transform"; we should perhaps change transform so it doesn't need the type?
        if (constraint!=null) {
            if (constraint.getExpectedKind()!=null && constraint.getExpectedKind()!=RegisteredTypeKind.SPEC) {
                throw new IllegalStateException("Cannot create spec with constraint "+constraint);
            }
            if (symbolicName != null && constraint.getAlreadyEncounteredTypes().contains(symbolicName)) {
                // avoid recursive cycle
                // TODO implement using java if permitted
            }
        }
        constraint = RegisteredTypeLoadingContexts.withSpecSuperType(constraint, specSuperType);

        Maybe<Object> result = TypePlanTransformers.transform(mgmt, type, constraint);
        if (result.isPresent()) return (SpecT) result.get();
        
        // fallback: look up in (legacy) catalog
        // TODO remove once all transformers are available in the new style
        CatalogItem item = symbolicName!=null ? (CatalogItem) mgmt.getCatalog().getCatalogItemLegacy(symbolicName, version) : null;
        if (item==null) {
            // if not in catalog (because loading a new item?) then look up item based on type
            // (only really used in tests; possibly also for any recursive legacy transformers we might have to create a CI; cross that bridge when we come to it)
            CatalogItemType ciType = CatalogItemType.ofTargetClass( (Class)constraint.getExpectedJavaSuperType() );
            if (ciType==null) {
                // throw -- not supported for non-spec types
                result.get();
            }
            item = CatalogItemBuilder.newItem(ciType, 
                    symbolicName!=null ? symbolicName : Identifiers.makeRandomId(8), 
                    version!=null ? version : BasicBrooklynCatalog.DEFAULT_VERSION)
                .plan((String)plan.getPlanData())
                .build();
        }
        try {
            return (SpecT) BasicBrooklynCatalog.internalCreateSpecLegacy(mgmt, item, constraint.getAlreadyEncounteredTypes(), false);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // for now, combine this failure with the original
            try {
                result.get();
                // above will throw -- so won't come here
                throw new IllegalStateException("should have failed getting type resolution for "+symbolicName);
            } catch (Exception e0) {
                Set<Exception> exceptionsInOrder = MutableSet.of();
                if (e0.toString().indexOf("none of the available transformers")>=0) {
                    // put the legacy exception first if none of the new transformers support the type
                    // (until the new transformer is the primary pathway)
                    exceptionsInOrder.add(e);
                    exceptionsInOrder.add(e0);
                } else {
                    exceptionsInOrder.add(e0);
                    exceptionsInOrder.add(e);
                }
                throw Exceptions.create("Unable to instantiate "+(symbolicName==null ? "item" : symbolicName), exceptionsInOrder); 
            }
        }
    }

    @Override
    public <SpecT extends AbstractBrooklynObjectSpec<?, ?>> SpecT createSpecFromPlan(@Nullable String planFormat, Object planData, @Nullable RegisteredTypeLoadingContext optionalConstraint, @Nullable Class<SpecT> optionalSpecSuperType) {
        return createSpec(RegisteredTypes.anonymousRegisteredType(RegisteredTypeKind.SPEC, new BasicTypeImplementationPlan(planFormat, planData)),
            optionalConstraint, optionalSpecSuperType);
    }

    @Override
    public <T> T createBean(RegisteredType type, @Nullable RegisteredTypeLoadingContext constraint, @Nullable Class<T> optionalResultSuperType) {
        Preconditions.checkNotNull(type, "type");
        if (type.getKind()!=RegisteredTypeKind.BEAN) { 
            if (type.getKind()==RegisteredTypeKind.UNRESOLVED) throw new ReferencedUnresolvedTypeException(type);
            else throw new UnsupportedTypePlanException("Cannot create bean from type "+type+" (kind "+type.getKind()+")");
        }
        if (constraint!=null) {
            if (constraint.getExpectedKind()!=null && constraint.getExpectedKind()!=RegisteredTypeKind.SPEC) {
                throw new IllegalStateException("Cannot create bean with constraint "+constraint);
            }
            if (constraint.getAlreadyEncounteredTypes().contains(type.getSymbolicName())) {
                // avoid recursive cycle
                // TODO create type using java if permitted?
                // OR remove this creator from those permitted
            }
        }
        constraint = RegisteredTypeLoadingContexts.withBeanSuperType(constraint, optionalResultSuperType);

        @SuppressWarnings("unchecked")
        T result = (T) TypePlanTransformers.transform(mgmt, type, constraint).get();
        return result;
    }

    @Override
    public <T> T createBeanFromPlan(String planFormat, Object planData, @Nullable RegisteredTypeLoadingContext optionalConstraint, @Nullable Class<T> optionalSuperType) {
        return createBean(RegisteredTypes.anonymousRegisteredType(RegisteredTypeKind.BEAN, new BasicTypeImplementationPlan(planFormat, planData)),
            optionalConstraint, optionalSuperType);
    }
    
    @Override
    public <T> T create(RegisteredType type, @Nullable RegisteredTypeLoadingContext constraint, @Nullable Class<T> optionalResultSuperType) {
        Preconditions.checkNotNull(type, "type");
        return new RegisteredTypeKindVisitor<T>() { 
            @Override protected T visitBean() { return createBean(type, constraint, optionalResultSuperType); }
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override protected T visitSpec() { return (T) createSpec(type, constraint, (Class)optionalResultSuperType); }
            @Override protected T visitUnresolved() {
                try {
                    // don't think there are valid times when this comes here?
                    // currently should only used for "templates" which are always for specs,
                    // but callers of that shouldn't be talking to type plan transformers,
                    // they should be calling to main BBTR methods.
                    // do it and alert just in case however.
                    // TODO remove if we don't see any warnings (or when we sort out semantics for template v app v allowed-unresolved better)
                    log.debug("Request for "+this+" to create UNRESOLVED kind "+type+"; trying as spec");
                    T result = visitSpec();
                    log.warn("Request to use "+this+" from UNRESOLVED state succeeded treating is as a spec");
                    log.debug("Trace for request to use "+this+" in UNRESOLVED state succeeding", new Throwable("Location of request to use "+this+" in UNRESOLVED state"));
                    return result;
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    throw new IllegalArgumentException("Kind-agnostic create method only intended for used when the registered type declares its kind, which "+type+" does not, "
                        + "and failed treating it as a spec: "+e, e);
                }
            }
        }.visit(type.getKind());
    }

    @Override
    public <T> T createFromPlan(Class<T> requiredSuperTypeHint, @Nullable String planFormat, Object planData, @Nullable RegisteredTypeLoadingContext optionalConstraint) {
        if (AbstractBrooklynObjectSpec.class.isAssignableFrom(requiredSuperTypeHint)) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            T result = (T) createSpecFromPlan(planFormat, planData, optionalConstraint, (Class)requiredSuperTypeHint);
            return result;
        }
        
        return createBeanFromPlan(planFormat, planData, optionalConstraint, requiredSuperTypeHint);
    }

    @Beta // API is stabilising
    public void addToLocalUnpersistedTypeRegistry(RegisteredType type, boolean canForce) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(type.getSymbolicName());
        Preconditions.checkNotNull(type.getVersion());
        Preconditions.checkNotNull(type.getId());
        if (!type.getId().equals(type.getSymbolicName()+":"+type.getVersion()))
            Asserts.fail("Registered type "+type+" has ID / symname mismatch");
        
        Locks.withLock(localRegistryLock.writeLock(),
            () -> {
                Map<String, RegisteredType> knownMatchingTypesByBundles = localRegisteredTypesAndContainingBundles.get(type.getId());
                if (knownMatchingTypesByBundles==null) {
                    knownMatchingTypesByBundles = MutableMap.of();
                    localRegisteredTypesAndContainingBundles.put(type.getId(), knownMatchingTypesByBundles);
                }

                Set<String> oldContainingBundlesToRemove = MutableSet.of();
                boolean newIsWrapperBundle = isWrapperBundle(type.getContainingBundle());
                for (RegisteredType existingT: knownMatchingTypesByBundles.values()) {
                    String reasonForDetailedCheck = null;
                    boolean sameBundle = Objects.equals(existingT.getContainingBundle(), type.getContainingBundle());
                    boolean oldIsWrapperBundle = isWrapperBundle(existingT.getContainingBundle());
                    if (sameBundle || (oldIsWrapperBundle && newIsWrapperBundle)) {
                        // allow replacement (different plan for same type) if either
                        // it's the same bundle or the old one was a wrapper, AND
                        // either we're forced or in snapshot-land
                        if (!sameBundle) {
                            // if old is wrapper bundle, we have to to remove the old record
                            oldContainingBundlesToRemove.add(existingT.getContainingBundle());
                        }
                        if (canForce) {
                            log.debug("Addition of "+type+" to replace "+existingT+" allowed because force is on");
                            continue;
                        }
                        if (BrooklynVersionSyntax.isSnapshot(type.getVersion())) {
                            if (existingT.getContainingBundle()!=null) {
                                if (BrooklynVersionSyntax.isSnapshot(VersionedName.fromString(existingT.getContainingBundle()).getVersionString())) {
                                    log.debug("Addition of "+type+" to replace "+existingT+" allowed because both are snapshot");
                                    continue;
                                } else {
                                    reasonForDetailedCheck = "the containing bundle "+existingT.getContainingBundle()+" is not a SNAPSHOT and addition is not forced";
                                }
                            } else {
                                // can this occur?
                                reasonForDetailedCheck = "the containing bundle of the type is unknown (cannot confirm it is snapshot)";
                            }
                        } else {
                            reasonForDetailedCheck = "the type is not a SNAPSHOT and addition is not forced";
                        }
                    } else if (oldIsWrapperBundle) {
                        reasonForDetailedCheck = type.getId()+" is in a named bundle replacing an item from an anonymous bundle-wrapped BOM, so definitions must be the same (or else give it a different version)";
                    } else if (newIsWrapperBundle) {
                        reasonForDetailedCheck = type.getId()+" is in an anonymous bundle-wrapped BOM replacing an item from a named bundle, so definitions must be the same (or else give it a different version)";
                    } else {
                        reasonForDetailedCheck = type.getId()+" is defined in different bundle";
                    }
                    assertSameEnoughToAllowReplacing(existingT, type, reasonForDetailedCheck);
                }
            
                log.debug("Inserting "+type+" into "+this+
                    (oldContainingBundlesToRemove.isEmpty() ? "" : " (removing entry from "+oldContainingBundlesToRemove+")"));
                for (String oldContainingBundle: oldContainingBundlesToRemove) {
                    knownMatchingTypesByBundles.remove(oldContainingBundle);
                }
                knownMatchingTypesByBundles.put(type.getContainingBundle(), type);
            });
    }

    private boolean isWrapperBundle(String bundleNameVersion) { 
        if (bundleNameVersion==null) return true;
        Maybe<OsgiManager> osgi = ((ManagementContextInternal)mgmt).getOsgiManager();
        // if not osgi, everything is treated as a wrapper bundle
        if (osgi.isAbsent()) return true;
        VersionedName vn = VersionedName.fromString(bundleNameVersion);
        Maybe<Bundle> b = osgi.get().findBundle(new BasicOsgiBundleWithUrl(vn.getSymbolicName(), vn.getOsgiVersionString(), null));
        // if bundle not found it is an error or a race; we don't fail, but we shouldn't treat it as a wrapper
        if (b.isAbsent()) return false;
        
        return BasicBrooklynCatalog.isWrapperBundle(b.get());
    }

    /**
     * Allow replacing even of non-SNAPSHOT versions if plans are "similar enough";
     * ie, forgiving some metadata changes.
     * <p>
     * This is needed when replacing an unresolved item with a resolved one or vice versa;
     * the {@link RegisteredType#equals(Object)} check is too strict.
     */
    private boolean assertSameEnoughToAllowReplacing(RegisteredType oldType, RegisteredType type, String reasonForDetailedCheck) {
       /* The bundle checksum check prevents swapping a different bundle at the same name+version.
        * For SNAPSHOT and forced-updates, this method doesn't apply, so we can assume here that
        * either the bundle checksums are the same,
        * or it is a different bundle declaring an item which is already installed.
        * <p>
        * Thus if the containing bundle is the same, the items are necessarily the same,
        * except for metadata we've mucked with (e.g. kind = unresolved).
        * <p>
        * If the containing bundle is different, it's possible someone is doing something sneaky.
        * If bundles aren't anonymous wrappers, then we should disallow --
        * e.g. a bundle BAR is declaring and item already declared by a bundle FOO.
        * 
        * 
        * It is the latter case we have to check.
        * 
        *  In the latter case we want to fail unless the old item comes from a wrapper bundle.
        * 
        * the only time this method 
        * applies where there might be differences in the item is if installing a bundle BAR which
        * declares an item with same name and version as an item already installed by a bundle FOO.
        *  
        * * uploading a bundle 
        * * uploading a BOM (no bundle) where the item it is defining is the same as one already defined   
        *  
        * (with the same non-SNAPSHOT version) bundle. So any changes to icons, plans, etc, should have already been caught;
        * this only applies if the BOM/bundle is identical, and in that case the only field where the two types here
        * could be different is the containing bundle metadata, viz. someone has uploaded an anonymous BOM twice. 
        */
        
        if (!oldType.getVersionedName().equals(type.getVersionedName())) {
            // different name - shouldn't even come here
            throw new IllegalStateException("Cannot add "+type+" to catalog; different "+oldType+" is already present ("+reasonForDetailedCheck+")");
        }
        if (Objects.equals(oldType.getContainingBundle(), type.getContainingBundle())) {
            // if named bundles equal then contents must be the same (due to bundle checksum); bail out early
            if (!samePlan(oldType, type)) {
                String msg = "Cannot add "+type+" to catalog; different plan in "+oldType+" from same bundle "+
                    type.getContainingBundle()+" is already present and "+reasonForDetailedCheck;
                log.debug(msg+"\n"+
                    "Plan being added is:\n"+type.getPlan()+"\n"+
                    "Plan already present is:\n"+oldType.getPlan() );
                throw new IllegalStateException(msg);
            }
            if (oldType.getKind()!=RegisteredTypeKind.UNRESOLVED && type.getKind()!=RegisteredTypeKind.UNRESOLVED &&
                    !Objects.equals(oldType.getKind(), type.getKind())) {
                throw new IllegalStateException("Cannot add "+type+" to catalog; different kind in "+oldType+" from same bundle is already present and "+reasonForDetailedCheck);
            }
            return true;
        }
        
        // different bundles, either anonymous or same item in two named bundles
        if (!samePlan(oldType, type)) {
            // if plan is different, fail
            String msg = "Cannot add "+type+" to catalog; it is different to "+oldType+", and "+reasonForDetailedCheck+" (throwing)";
            log.debug(msg+"\n"+
                "Plan being added from "+type.getContainingBundle()+" is:\n"+type.getPlan()+"\n"+
                "Plan already present from "+oldType.getContainingBundle()+" is:\n"+oldType.getPlan() );
            throw new IllegalStateException(msg);
        }
        if (oldType.getKind()!=RegisteredTypeKind.UNRESOLVED && type.getKind()!=RegisteredTypeKind.UNRESOLVED &&
                !Objects.equals(oldType.getKind(), type.getKind())) {
            // if kind is different and both resolved, fail
            throw new IllegalStateException("Cannot add "+type+" to catalog; it is a different kind to "+oldType+", and "
                + reasonForDetailedCheck);
        }

        // it is the same plan as a type in another bundle;
        // previously we allowed only when the other bundle was a wrapper or no bundle
        // (or it was forced - which was guaranteed to cause problems on rebind!);
        // but now we always allow it, and we track which bundles things come from
        return true;
    }

    private boolean samePlan(RegisteredType oldType, RegisteredType type) {
        return RegisteredTypes.arePlansEquivalent(oldType, type);
    }

    /** removes registered type with the given ID from the local in-memory catalog
     * (ignores bundle, deletes from all bundles if present in multiple; 
     * falls back to removing from legacy catalog if not known here in any bundles)
     * @throws NoSuchElementException if not found */
    @Beta // API stabilising
    public void delete(VersionedName type) {
        boolean changedLocally = Locks.withLock(localRegistryLock.writeLock(),
            () -> (localRegisteredTypesAndContainingBundles.remove(type.toString()) != null));
        if (changedLocally) {
            return;
        }
        legacyDelete(type);
    }
    
    @SuppressWarnings("deprecation")
    private void legacyDelete(VersionedName type) {
        // when we delete this, we should simply do the following (as contract is to throw if can't delete)
//        if (Strings.isBlank(type.getVersionString()) || BrooklynCatalog.DEFAULT_VERSION.equals(type.getVersionString())) {
//            throw new IllegalStateException("Deleting items with unspecified version (argument DEFAULT_VERSION) not supported.");
//        }
//        throw new NoSuchElementException("No catalog item found with id "+type);
        
        // NB the call below may call back to us, but max once)
        mgmt.getCatalog().deleteCatalogItem(type.getSymbolicName(), type.getVersionString());
    }
    
    /** removes the given registered type in the noted bundle; 
     * if not known in that bundle tries deleting from legacy catalog.
     * @throws NoSuchElementException if not found */
    public void delete(RegisteredType type) {
        boolean changedLocally = Locks.withLock(localRegistryLock.writeLock(),
            () -> {
                Map<String, RegisteredType> m = localRegisteredTypesAndContainingBundles.get(type.getId());
                if (m==null) return false;
                RegisteredType removedItem = m.remove(type.getContainingBundle());
                if (m.isEmpty()) {
                    localRegisteredTypesAndContainingBundles.remove(type.getId());
                }
                if (removedItem==null) {
                    throw new NoSuchElementException("Requested to delete "+type+" from "+type.getContainingBundle()+", "
                        + "but that type was not known in that bundle, it is in "+m.keySet()+" instead");
                }
                return true;
            });
        if (changedLocally) {
            return;
        }
        
        legacyDelete(type.getVersionedName());
    }
    
    /** as {@link #delete(VersionedName)} */
    @Beta // API stabilising
    public void delete(String id) {
        delete(VersionedName.fromString(id));
    }
    
}
