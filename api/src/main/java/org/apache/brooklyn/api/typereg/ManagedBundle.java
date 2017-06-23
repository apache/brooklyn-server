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

import org.apache.brooklyn.api.mgmt.rebind.Rebindable;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.util.osgi.VersionedName;

/** Describes an OSGi bundle which Brooklyn manages, including persisting */
public interface ManagedBundle extends BrooklynObject, Rebindable, OsgiBundleWithUrl {

    /** A URL-like thing that we can register with the OSGi framework
     * to uniquely identify this bundle-instance.
     * This typically includes the unique {@link #getId()} of this item. */
    String getOsgiUniqueUrl();

    VersionedName getVersionedName();

}
