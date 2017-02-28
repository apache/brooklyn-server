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
package org.apache.brooklyn.location.jclouds.api;

import java.util.List;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineManagementMixins;
import org.apache.brooklyn.core.mgmt.persist.LocationWithObjectStore;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;

import com.google.common.collect.ImmutableList;

/** A public interface in front of Jclouds Location for use from downstream projects which wish
 * to reference JcloudsLocation but not have a package-dependency on org.apache.brooklyn.location.jclouds
 * (which pulls in many of the jclouds bundles) */
public interface JcloudsLocationPublic extends 
    JcloudsLocationConfig, MachineManagementMixins.RichMachineProvisioningLocation<MachineLocation>,
    LocationWithObjectStore, MachineManagementMixins.SuspendResumeLocation 
{

    public static final String ROOT_USERNAME = "root";
    /** these userNames are known to be the preferred/required logins in some common/default images
     *  where root@ is not allowed to log in */
    public static final List<String> ROOT_ALIASES = ImmutableList.of("ubuntu", "centos", "ec2-user");
    public static final List<String> COMMON_USER_NAMES_TO_TRY = ImmutableList.<String>builder().add(ROOT_USERNAME).addAll(ROOT_ALIASES).add("admin").build();

    public String getProvider();
    
    public String getIdentity();
    public String getCredential();
    
    /** returns the location ID used by the provider, if set, e.g. us-west-1 */
    public String getRegion();

    public String getEndpoint();
    
    /** return a nice string suitable for display messages */
    public String toStringNice();
    
}
