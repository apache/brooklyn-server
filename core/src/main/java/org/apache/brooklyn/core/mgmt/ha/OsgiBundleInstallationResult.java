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
import org.apache.brooklyn.util.collections.MutableList;
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
        IGNORING_BUNDLE_AREADY_INSTALLED(false), 
        ERROR_PREPARING_BUNDLE(true),
        ERROR_INSTALLING_BUNDLE(true);
        
        final boolean isError;
        ResultCode(boolean isError) { this.isError = isError; }
        
        public boolean isError() { return isError; }
    }
    final List<String> catalogItemsInstalled = MutableList.of();
    
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
    
    @Override
    public String toString() {
        return OsgiBundleInstallationResult.class.getSimpleName()+"["+code+", "+metadata+", "+message+"]";
    }
}