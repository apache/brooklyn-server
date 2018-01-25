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
package org.apache.brooklyn.core.mgmt.ha;

import java.util.List;

import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.osgi.framework.Bundle;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;

@Beta
public class OsgiBundleInstallationResult {
    String message;
    ManagedBundle metadata;
    Bundle bundle;
    ResultCode code;
    Runnable deferredStart;
    
    public enum ResultCode { 
        INSTALLED_NEW_BUNDLE(false),
        UPDATED_EXISTING_BUNDLE(false),

        /** Bundle is already installed at exact same version and same contents; safely ignoring
         * (safe in that behaviour won't be different or dangerous; 
         * could potentially be surprising, but ability to idempotently install things is nicer) */
        IGNORING_BUNDLE_AREADY_INSTALLED(false),
        /** Bundle has been forcibly removed; not installing; 
         * bundle metadata for an upgrade may be returned, if there is one */
        IGNORING_BUNDLE_FORCIBLY_REMOVED(false),
        /** bundle could not be made insto a state where it could be installed; bundle is not installed, even if forced */
        ERROR_PREPARING_BUNDLE(true),
        /** bundle successfully installed to OSGi container but there was an error launching it, 
         * either the OSGi bundle start, catalog items load, or (most commonly) validating the catalog items;
         * bundle may be installed (currently it is in most/all places, but behaviour TBC) so caller may have to uninstall it */
        ERROR_LAUNCHING_BUNDLE(true),
        // codes below used for deletion
        BUNDLE_REMOVED(false),
        ERROR_REMOVING_BUNDLE_IN_USE(true),
        ERROR_REMOVING_BUNDLE_OTHER(true);
        
        final boolean isError;
        ResultCode(boolean isError) { this.isError = isError; }
        
        public boolean isError() { return isError; }
    }
    final List<RegisteredType> typesInstalled = MutableList.of();
    /** @deprecated since 1.0.0 use {@link #typesInstalled} */
    @Deprecated
    private final List<String> catalogItemsInstalled = MutableList.of();
    
    public String getMessage() {
        return message;
    }
    public Bundle getBundle() {
        return bundle;
    }
    public ManagedBundle getMetadata() {
        return metadata;
    }
    public ResultCode getCode() {
        return code;
    }
    public List<RegisteredType> getTypesInstalled() {
        return typesInstalled;
    }
    /** @deprecated since 1.0.0 use {@link #getTypesInstalled()} */
    @Deprecated
    public List<String> getCatalogItemsInstalled() {
        return ImmutableList.copyOf(catalogItemsInstalled);
    }
    public VersionedName getVersionedName() {
        if (getMetadata()==null) return null;
        return getMetadata().getVersionedName();
    }
    public Runnable getDeferredStart() {
        return deferredStart;
    }
    
    void setIgnoringAlreadyInstalled() {
        code = OsgiBundleInstallationResult.ResultCode.IGNORING_BUNDLE_AREADY_INSTALLED;
        message = "Bundle "+getMetadata().getVersionedName()+" already installed as "+getMetadata().getId();
    }
    
    void setIgnoringForciblyRemoved(VersionedName requestedBundle, Maybe<VersionedName> replacementBundle) {
        code = OsgiBundleInstallationResult.ResultCode.IGNORING_BUNDLE_FORCIBLY_REMOVED;
        message = "Bundle "+requestedBundle+" forcibly removed, "
                +(replacementBundle.isPresentAndNonNull() ? "upgraded to "+replacementBundle.get().getOsgiVersionString() : "no upgrade defined");
    }
    
    @Override
    public String toString() {
        return OsgiBundleInstallationResult.class.getSimpleName()+"["+code+", "+metadata+", "+message+"]";
    }
    public void addType(RegisteredType ci) {
        typesInstalled.add(ci);
        catalogItemsInstalled.add(ci.getId());        
    }
}