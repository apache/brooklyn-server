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

package org.apache.brooklyn.location.jclouds.templates.customize;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.cloudstack.compute.options.CloudStackTemplateOptions;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NetworkNameOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkNameOption.class);

    @Override
    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        if (t instanceof AWSEC2TemplateOptions) {
            // subnet ID is the sensible interpretation of network name in EC2
            ((AWSEC2TemplateOptions) t).subnetId((String) v);

        } else {
            if (isGoogleComputeTemplateOptions(t)) {
                // no warning needed
                // we think this is the only jclouds endpoint which supports this option

            } else if (t instanceof SoftLayerTemplateOptions) {
                LOG.warn("networkName is not be supported in SoftLayer; use `templateOptions` with `primaryNetworkComponentNetworkVlanId` or `primaryNetworkBackendComponentNetworkVlanId`");
            } else if (!(t instanceof CloudStackTemplateOptions) && !(t instanceof NovaTemplateOptions)) {
                LOG.warn("networkName is experimental in many jclouds endpoints may not be supported in this cloud");
                // NB, from @andreaturli
//                                Cloudstack uses custom securityGroupIds and networkIds not the generic networks
//                                Openstack Nova uses securityGroupNames which is marked as @deprecated (suggests to use groups which is maybe even more confusing)
//                                Azure supports the custom networkSecurityGroupName
            }

            t.networks((String) v);
        }
    }

    /**
     * Avoid having a dependency on googlecompute because it doesn't have an OSGi bundle yet.
     * Fixed in jclouds 2.0.0-SNAPSHOT
     */
    private static boolean isGoogleComputeTemplateOptions(TemplateOptions t) {
        return t.getClass().getName().equals("org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions");
    }
}
