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
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SecurityGroupOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(SecurityGroupOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        if (t instanceof EC2TemplateOptions) {
            String[] securityGroups = toStringArray(v);
            ((EC2TemplateOptions) t).securityGroups(securityGroups);
        } else if (t instanceof NovaTemplateOptions) {
            String[] securityGroups = toStringArray(v);
            t.securityGroups(securityGroups);
        } else if (t instanceof SoftLayerTemplateOptions) {
            String[] securityGroups = toStringArray(v);
            t.securityGroups(securityGroups);
        } else if (isGoogleComputeTemplateOptions(t)) {
            String[] securityGroups = toStringArray(v);
            t.securityGroups(securityGroups);
        } else {
            LOG.info("ignoring securityGroups({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
        }
    }

    private String[] toStringArray(Object v) {
        return Strings.toStringList(v).toArray(new String[0]);
    }

    /**
     * Avoid having a dependency on googlecompute because it doesn't have an OSGi bundle yet.
     * Fixed in jclouds 2.0.0-SNAPSHOT
     */
    private static boolean isGoogleComputeTemplateOptions(TemplateOptions t) {
        return t.getClass().getName().equals("org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions");
    }
}
