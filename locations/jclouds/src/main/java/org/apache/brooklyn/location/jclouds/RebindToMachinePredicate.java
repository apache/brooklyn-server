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

package org.apache.brooklyn.location.jclouds;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;

/**
 * Determines whether a machine may be rebinded to by comparing the given id, hostname and region
 * against the node's id, hostname, provider id and public addresses.
 */
class RebindToMachinePredicate implements Predicate<ComputeMetadata> {

    final String rawId;
    final String rawHostname;
    final String rawRegion;

    public RebindToMachinePredicate(ConfigBag config) {
        rawId = (String) config.getStringKey("id");
        rawHostname = (String) config.getStringKey("hostname");
        rawRegion = (String) config.getStringKey("region");
    }

    @Override
    public boolean apply(ComputeMetadata input) {
        // ID exact match
        if (rawId != null) {
            // Second is AWS format
            if (rawId.equals(input.getId()) || rawRegion != null && (rawRegion + "/" + rawId).equals(input.getId())) {
                return true;
            }
        }

        // else do node metadata lookup
        if (input instanceof NodeMetadata) {
            NodeMetadata node = NodeMetadata.class.cast(input);
            if (rawHostname != null && rawHostname.equalsIgnoreCase(node.getHostname()))
                return true;
            if (rawHostname != null && node.getPublicAddresses().contains(rawHostname))
                return true;
            if (rawId != null && rawId.equalsIgnoreCase(node.getHostname()))
                return true;
            if (rawId != null && node.getPublicAddresses().contains(rawId))
                return true;
            // don't do private IPs because they might be repeated
            if (rawId != null && rawId.equalsIgnoreCase(node.getProviderId()))
                return true;
            if (rawHostname != null && rawHostname.equalsIgnoreCase(node.getProviderId()))
                return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .omitNullValues()
                .add("id", rawId)
                .add("hostname", rawHostname)
                .add("region", rawRegion)
                .toString();
    }
}
