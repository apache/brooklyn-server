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
package org.apache.brooklyn.core.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.StringWriter;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.rebind.mementos.Memento;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializer;
import org.apache.brooklyn.core.mgmt.rebind.dto.MementosGenerators;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

public class CatalogPredicates {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogPredicates.class);

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> isCatalogItemType(final CatalogItemType ciType) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && item.getCatalogItemType()==ciType;
            }
        };
        return new CatalogItemTypeEqualTo<T, SpecT>(ciType);
    }

    /**
     * @since 0.8.0
     */
    private static class CatalogItemTypeEqualTo<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final CatalogItemType ciType;
        
        public CatalogItemTypeEqualTo(final CatalogItemType ciType) {
            this.ciType = ciType;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && item.getCatalogItemType()==ciType;
        }
        @Override
        public String toString() {
            return "CatalogItemTypeEqualTo("+ciType+")";
        }
    }

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> deprecated(final boolean deprecated) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && item.isDeprecated() == deprecated;
            }
        };
        return new DeprecatedEqualTo<T, SpecT>(deprecated);
    }

    /**
     * @since 0.8.0
     */
    private static class DeprecatedEqualTo<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final boolean deprecated;
        
        public DeprecatedEqualTo(boolean deprecated) {
            this.deprecated = deprecated;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && item.isDeprecated() == deprecated;
        }
        @Override
        public String toString() {
            return "DeprecatedEqualTo("+deprecated+")";
        }
    }

    /**
     * @since 0.8.0
     */
    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> disabled(boolean disabled) {
        return new DisabledEqualTo<T, SpecT>(disabled);
    }

    /**
     * @since 0.8.0
     */
    private static class DisabledEqualTo<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final boolean disabled;
        
        public DisabledEqualTo(boolean disabled) {
            this.disabled = disabled;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && item.isDisabled() == disabled;
        }
        @Override
        public String toString() {
            return "DisabledEqualTo("+disabled+")";
        }
    }

    public static final Predicate<CatalogItem<Application,EntitySpec<? extends Application>>> IS_TEMPLATE = 
            CatalogPredicates.<Application,EntitySpec<? extends Application>>isCatalogItemType(CatalogItemType.TEMPLATE);
    public static final Predicate<CatalogItem<Entity,EntitySpec<?>>> IS_ENTITY = 
            CatalogPredicates.<Entity,EntitySpec<?>>isCatalogItemType(CatalogItemType.ENTITY);
    public static final Predicate<CatalogItem<Policy,PolicySpec<?>>> IS_POLICY = 
            CatalogPredicates.<Policy,PolicySpec<?>>isCatalogItemType(CatalogItemType.POLICY);
    public static final Predicate<CatalogItem<Enricher,EnricherSpec<?>>> IS_ENRICHER =
            CatalogPredicates.<Enricher,EnricherSpec<?>>isCatalogItemType(CatalogItemType.ENRICHER);
    public static final Predicate<CatalogItem<Location,LocationSpec<?>>> IS_LOCATION = 
            CatalogPredicates.<Location,LocationSpec<?>>isCatalogItemType(CatalogItemType.LOCATION);
    
    // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
    @SuppressWarnings("unused")
    private static final Function<CatalogItem<?,?>,String> ID_OF_ITEM_TRANSFORMER_ANONYMOUS = new Function<CatalogItem<?,?>, String>() {
        @Override @Nullable
        public String apply(@Nullable CatalogItem<?,?> input) {
            if (input==null) return null;
            return input.getId();
        }
    };

    // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
    public static final Function<CatalogItem<?,?>,String> ID_OF_ITEM_TRANSFORMER = new IdOfItemTransformer();
    
    /**
     * @since 0.8.0
     */
    private static class IdOfItemTransformer implements Function<CatalogItem<?,?>,String> {
        @Override @Nullable
        public String apply(@Nullable CatalogItem<?,?> input) {
            if (input==null) return null;
            return input.getId();
        }
    };

    /** @deprecated since 0.7.0 use {@link #displayName(Predicate)} */
    @Deprecated
    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> name(final Predicate<? super String> filter) {
        return displayName(filter);
    }

    /**
     * @since 0.7.0
     */
    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> displayName(final Predicate<? super String> filter) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && filter.apply(item.getDisplayName());
            }
        };
        return new DisplayNameMatches<T,SpecT>(filter);
    }

    /**
     * @since 0.8.0
     */
    private static class DisplayNameMatches<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final Predicate<? super String> filter;
        
        public DisplayNameMatches(Predicate<? super String> filter) {
            this.filter = filter;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && filter.apply(item.getDisplayName());
        }
        @Override
        public String toString() {
            return "DisplayNameMatches("+filter+")";
        }
    }

    @Deprecated
    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> registeredTypeName(final Predicate<? super String> filter) {
        return symbolicName(filter);
    }

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> symbolicName(final Predicate<? super String> filter) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && filter.apply(item.getSymbolicName());
            }
        };
        return new SymbolicNameMatches<T,SpecT>(filter);
    }
    
    /**
     * @since 0.8.0
     */
    private static class SymbolicNameMatches<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final Predicate<? super String> filter;
        
        public SymbolicNameMatches(Predicate<? super String> filter) {
            this.filter = filter;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && filter.apply(item.getSymbolicName());
        }
        @Override
        public String toString() {
            return "SymbolicNameMatches("+filter+")";
        }
    }

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> javaType(final Predicate<? super String> filter) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && filter.apply(item.getJavaType());
            }
        };
        return new JavaTypeMatches<T, SpecT>(filter);
    }
    
    /**
     * @since 0.8.0
     */
    private static class JavaTypeMatches<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final Predicate<? super String> filter;
        
        public JavaTypeMatches(Predicate<? super String> filter) {
            this.filter = filter;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && filter.apply(item.getJavaType());
        }
        @Override
        public String toString() {
            return "JavaTypeMatches("+filter+")";
        }
    }

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> stringRepresentationMatches(final Predicate<? super String> filter) {
        // TODO Previously the impl relied on catalogItem.toXmlString() to get a string 
        // representation, which the filter could then be applied to. We've deleted that
        // (as part of deleting support for ~/.brooklyn/catalog.xml, which was deprecated
        // back in 0.7.0).
        // Now we piggy-back off the persistence serialization. However, we really don't
        // want to let users rely on that! We'll presumably swap out the string representation
        // at some point in the future.
        return new StringRepresentationMatches<>(checkNotNull(filter, "filter"));
    }
    private static class StringRepresentationMatches<T, SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final Predicate<? super String> filter;
        StringRepresentationMatches(final Predicate<? super String> filter) {
            this.filter = filter;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            try {
                Memento memento = MementosGenerators.newBasicMemento(item);
                XmlMementoSerializer<CatalogItem<?,?>> serializer = new XmlMementoSerializer<CatalogItem<?,?>>(
                        CatalogPredicates.class.getClassLoader(), 
                        ImmutableMap.<String,String>of());
                StringWriter writer = new StringWriter();
                serializer.serialize(memento, writer);
                return filter.apply(writer.toString());
            } catch (Exception e) {
                // If we propagated exceptions, then we'd risk aborting the checks for other catalog items.
                // Play it safe, in case there's something messed up with just one catalog item.
                Exceptions.propagateIfFatal(e);
                LOG.debug("Problem producing string representation of "+item+"; assuming no match, and continuing", e);
                return false;
            }
        }
    }

    @SuppressWarnings("unused")
    private static <T,SpecT> Predicate<CatalogItem<T,SpecT>> xml(final Predicate<? super String> filter) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                throw new IllegalStateException();
            }
        };
        throw new UnsupportedOperationException();
    }
    
    /**
     * @since 0.8.0
     * @deprecated since 0.11.0 (should have been 0.7.0, when catalog.xml was deprecated!)
     * 
     * TODO Kept for backwards compatibility, in case it is reference in customer persisted state.
     */
    @SuppressWarnings("unused")
    private static class XmlMatches<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final Predicate<? super String> filter;
        
        public XmlMatches(Predicate<? super String> filter) {
            throw new IllegalStateException();
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            throw new IllegalStateException();
        }
        @Override
        public String toString() {
            return "XmlMatches("+filter+")";
        }
    }

    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> entitledToSee(final ManagementContext mgmt) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return (item != null) && 
                    Entitlements.isEntitled(mgmt.getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, item.getCatalogItemId());
            }
        };
        return new EntitledToSee<T, SpecT>(mgmt);
    }
    
    /**
     * @since 0.8.0
     */
    private static class EntitledToSee<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final ManagementContext mgmt;
        
        public EntitledToSee(ManagementContext mgmt) {
            this.mgmt = mgmt;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return (item != null) && 
                    Entitlements.isEntitled(mgmt.getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, item.getCatalogItemId());
        }
        @Override
        public String toString() {
            return "EntitledToSee()";
        }
    }
 
    public static <T,SpecT> Predicate<CatalogItem<T,SpecT>> isBestVersion(final ManagementContext mgmt) {
        // TODO PERSISTENCE WORKAROUND kept anonymous function in case referenced in persisted state
        new Predicate<CatalogItem<T,SpecT>>() {
            @Override
            public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
                return CatalogUtils.isBestVersion(mgmt, item);
            }
        };
        return new IsBestVersion<T, SpecT>(mgmt);
    }
    
    /**
     * @since 0.8.0
     */
    private static class IsBestVersion<T,SpecT> implements Predicate<CatalogItem<T,SpecT>> {
        private final ManagementContext mgmt;
        
        public IsBestVersion(ManagementContext mgmt) {
            this.mgmt = mgmt;
        }
        @Override
        public boolean apply(@Nullable CatalogItem<T,SpecT> item) {
            return CatalogUtils.isBestVersion(mgmt, item);
        }
        @Override
        public String toString() {
            return "IsBestVersion()";
        }
    }
}