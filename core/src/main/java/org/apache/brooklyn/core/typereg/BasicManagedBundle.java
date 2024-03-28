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

import java.util.Map;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.mgmt.rebind.BasicManagedBundleRebindSupport;
import org.apache.brooklyn.core.objs.AbstractBrooklynObject;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.http.auth.Credentials;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BasicManagedBundle extends AbstractBrooklynObject implements ManagedBundle, BrooklynObjectInternal {

    private String symbolicName;
    private String version;
    private String checksum;
    private Boolean deleteable;
    private String osgiUniqueUrl;
    private String format;
    private String url;
    private Credentials credentials;
    private Boolean fromInitialCatalog;

    /** pretty much redundant as it is put in the delta if changed, and included even if not needed when full checkpoint requested */
    private transient boolean persistenceNeeded = false;


    /** Creates an empty one, with an ID, expecting other fields will be populated. */
    public BasicManagedBundle() {}

    /** @deprecated since 1.1 use larger constructor */ @Deprecated
    public BasicManagedBundle(String name, String version, String url, @Nullable String checksum) {
        this(name, version, url, null, null, checksum);
    }
    /** @deprecated since 1.1 use larger constructor */ @Deprecated
    public BasicManagedBundle(String name, String version, String url, Credentials credentials, @Nullable String checksum) {
        this(name, version, url, null, credentials, checksum);
    }

    /** @deprecated since 1.1 use larger constructor */ @Deprecated
    public BasicManagedBundle(String name, String version, String url, String format, Credentials credentials, @Nullable String checksum) {
        init(name, version, url, format, credentials, checksum, null, null);
    }

    public BasicManagedBundle(String name, String version, String url, String format, Credentials credentials, @Nullable String checksum, @Nullable Boolean deleteable) {
        init(name, version, url, format, credentials, checksum, deleteable, null);
    }
    public BasicManagedBundle(String name, String version, String url, String format, Credentials credentials, @Nullable String checksum, @Nullable Boolean deleteable, @Nullable Boolean fromInitialCatalog) {
        init(name, version, url, format, credentials, checksum, deleteable, fromInitialCatalog);
    }

    private void init(String name, String version, String url, String format, Credentials credentials, @Nullable String checksum, @Nullable Boolean deleteable, @Nullable Boolean fromInitialCatalog) {
        if (name == null && version == null) {
            Preconditions.checkNotNull(url, "Either a URL or both name and version are required");
        } else {
            Preconditions.checkNotNull(name, "Either a URL or both name and version are required");
            Preconditions.checkNotNull(version, "Either a URL or both name and version are required");
        }
        this.symbolicName = name;
        this.version = version;
        this.url = url;
        this.format = format;
        this.credentials = credentials;
        this.checksum = checksum;
        this.deleteable = deleteable;
        this.fromInitialCatalog = fromInitialCatalog;
    }

    private BasicManagedBundle(String id, String name, String version, String url, String format, Credentials credentials, @Nullable String checksum, @Nullable Boolean deleteable, @Nullable Boolean fromInitialCatalog) {
        super(id);
        init(name, version, url, format, credentials, checksum, deleteable, fromInitialCatalog);
    }

    /** used when updating a persisted bundle, we want to use the coords (ID and OSGI unique URL) of the second with the checksum of the former;
     * the other fields should be the same between the two but if in doubt use the first argument
     */
    public static BasicManagedBundle copyFirstWithCoordsOfSecond(ManagedBundle update, ManagedBundle oldOneForCoordinates) {
        BasicManagedBundle result = new BasicManagedBundle(oldOneForCoordinates.getId(), update.getSymbolicName(), update.getSuppliedVersionString(), update.getUrl(), update.getFormat(), update.getUrlCredential(), update.getChecksum(), update.getDeleteable(),
                update instanceof BasicManagedBundle ? ((BasicManagedBundle)update).getFromInitialCatalog() : null);
        result.tags().addTags(update.tags().getTags());
        // we have secondary logic which should accept a change in the OSGi unique URL,
        // but more efficient if we use the original URL
        result.osgiUniqueUrl = oldOneForCoordinates.getOsgiUniqueUrl();
        return result;
    }
    
    @Override
    public boolean isNameResolved() {
        return symbolicName != null && version != null;
    }

    public void setDeleteable(Boolean deleteable) {
        this.deleteable = deleteable;
    }
    @Override
    public Boolean getDeleteable() {
        return deleteable;
    }

    @Override
    public String getSymbolicName() {
        return symbolicName;
    }

    public void setSymbolicName(String symbolicName) {
        this.symbolicName = symbolicName;
    }
    
    @Override
    public String getSuppliedVersionString() {
        return version;
    }

    @Override
    public String getOsgiVersionString() {
        return version==null ? null : BrooklynVersionSyntax.toValidOsgiVersion(version);
    }

    public void setVersion(String version) {
        this.version = version;
    }
    
    @Override
    public VersionedName getVersionedName() {
        if (symbolicName==null) return null;
        return new VersionedName(symbolicName, version);
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public Credentials getUrlCredential() {
        return credentials;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Gets the (internal) value to be used as the location in bundleContext.install(location) / bundleContext.getBundle(location).
     * It thus allows us to tell if a cached OSGi bundle should be considered as replacement
     * for the one we are about to install (e.g. one we get from persisted state),
     * or have retrieved from the initial catalog with a unique URL for a bundle symbolic-name + version.
     * This is typically not a real URL, as the same bundle may come from various locations.
     * Typically we use a checksum or internal ID.
     * In the case of same-version (eg snapshot or forced) bundle updates we remember and re-use the _original_ checksum.
     * However code should be tolerant if this is not unique and use other more-expensive mechanisms for secondary de-duplication.
     * 
     * Care should be taken to set the checksum <em>before</em> using the OSGi unique url.
     */
    @Override
    public String getOsgiUniqueUrl() {
        if (osgiUniqueUrl==null) {
            osgiUniqueUrl = "brooklyn:" + (checksum != null ? checksum : getId());
        }
        return osgiUniqueUrl;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("symbolicName", symbolicName)
                .add("version", version)
                .add("format", format)
                .add("url", url)
                .add("osgiUniqueUrl", osgiUniqueUrl)
                .toString();
    }

    @Override
    public int hashCode() {
        // checksum deliberately omitted here to match with OsgiBundleWithUrl
        return Objects.hashCode(symbolicName, getOsgiVersionString(), url);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        OsgiBundleWithUrl other = (OsgiBundleWithUrl) obj;
        if (!Objects.equal(symbolicName, other.getSymbolicName())) return false;
        if (!Objects.equal(getOsgiVersionString(), other.getOsgiVersionString())) return false;
        if (!Objects.equal(url, other.getUrl())) return false;
        if (other instanceof ManagedBundle) {
            // checksum compared if available, but not required;
            // this makes equality with other OsgiBundleWithUrl items symmetric,
            // but for two MB's we look additionally at checksum
            if (!Objects.equal(checksum, ((ManagedBundle)other).getChecksum())) return false;
            
            // only equal if have the same ManagedBundle uid; important for persistence.changeListener().unmanage()
            if (!Objects.equal(getId(), ((ManagedBundle)other).getId())) return false;
        }
        return true;
    }

    // ---
    
    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RebindSupport<?> getRebindSupport() {
        return new BasicManagedBundleRebindSupport(this);
    }

    @Override
    public ConfigurationSupportInternal config() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SubscriptionSupportInternal subscriptions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDisplayName(String newName) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public String getChecksum() {
        return checksum;
    }
    
    public void setChecksum(String md5Checksum) {
        this.checksum = md5Checksum;
    }

    @Override
    protected BrooklynObjectInternal configure(Map<?, ?> flags) {
        throw new UnsupportedOperationException();
    }

    public static ManagedBundle of(OsgiBundleWithUrl bundle) {
        String checksum = (bundle instanceof ManagedBundle) ? ((ManagedBundle)bundle).getChecksum() : null;
        return new BasicManagedBundle(
                bundle.getSymbolicName(),
                bundle.getSuppliedVersionString(),
                bundle.getUrl(),
                null,
                bundle.getUrlCredential(),
                checksum,
                bundle.getDeleteable(),
                (bundle instanceof BasicManagedBundle ? ((BasicManagedBundle)bundle).getFromInitialCatalog() : null));
    }

    public void setPersistenceNeeded(boolean val) {
        persistenceNeeded = val;
    }
    public boolean getPersistenceNeeded() {
        return persistenceNeeded;
        
    }

    public Boolean getFromInitialCatalog() {
        return fromInitialCatalog;
    }
    public void setFromInitialCatalog(Boolean fromInitialCatalog) {
        this.fromInitialCatalog = fromInitialCatalog;
    }
}
