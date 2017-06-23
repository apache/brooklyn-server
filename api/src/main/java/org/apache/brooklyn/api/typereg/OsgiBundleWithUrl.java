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

import org.apache.brooklyn.util.osgi.VersionedName;

import com.google.common.annotations.Beta;

@Beta
public interface OsgiBundleWithUrl {
    
    public String getSymbolicName();
    /** guaranteed to be in OSGi syntax, or null if name+version not available */
    public String getOsgiVersionString();
    
    /** string as defined by caller; not necessarily in OSGi syntax */
    public String getSuppliedVersionString();
    
    /** where this bundle can be downloaded; typically required unless we are guaranteed the bundle will be manually installed or handled by persistence */
    public String getUrl();
    
    /** @return true if we have a name and version for this bundle;
     * false if not, e.g. if we only know the URL and we haven't loaded it yet */
    public boolean isNameResolved();
    
    /** @return the {@link VersionedName} for this bundle, or null if not available */
    public VersionedName getVersionedName();

}
