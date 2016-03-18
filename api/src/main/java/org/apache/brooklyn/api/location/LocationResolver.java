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
package org.apache.brooklyn.api.location;

import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;

/**
 * Provides a way of creating location instances of a particular type.
 */
public interface LocationResolver {

    void init(ManagementContext managementContext);
    
    /** the prefix that this resolver will attend to */
    String getPrefix();
    
    /** whether the spec is something which should be passed to this resolver */
    boolean accepts(String spec, LocationRegistry registry);

    /** whether the location is enabled */
    boolean isEnabled();

    /**
     * Creates a LocationSpec given a spec string, flags (e.g. user, key, cloud credential),
     * and the registry itself from which the base properties are discovered
     * and supporting locations which refer to other locations, e.g. NamedLocationResolver.
     **/ 
    LocationSpec<? extends Location> newLocationSpecFromString(String spec, Map<?,?> locationFlags, LocationRegistry registry);

}
