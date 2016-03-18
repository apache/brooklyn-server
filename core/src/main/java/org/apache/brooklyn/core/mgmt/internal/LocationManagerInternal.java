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
package org.apache.brooklyn.core.mgmt.internal;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.LocationManager;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

public interface LocationManagerInternal extends LocationManager, BrooklynObjectManagerInternal<Location> {

    /** Indicates that a call to {@link #createLocation(LocationSpec)} should create a location instance not
     * linked to the management context. This allows clients to peek at the result but not to use it. */
    public static final ConfigKey<Boolean> CREATE_UNMANAGED = ConfigKeys.newBooleanConfigKey("brooklyn.internal.location.createUnmanaged",
        "If set on a location or spec, causes the manager to create it in an unmanaged state (for peeking)", false);

    public Iterable<String> getLocationIds();

}
