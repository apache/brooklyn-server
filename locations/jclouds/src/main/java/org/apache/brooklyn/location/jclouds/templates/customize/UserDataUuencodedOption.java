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
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UserDataUuencodedOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(UserDataUuencodedOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        if (t instanceof EC2TemplateOptions) {
            byte[] bytes = toByteArray(v);
            ((EC2TemplateOptions) t).userData(bytes);
        } else if (t instanceof SoftLayerTemplateOptions) {
            ((SoftLayerTemplateOptions) t).userData(Strings.toString(v));
        } else {
            LOG.info("ignoring userData({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
        }
    }

    private byte[] toByteArray(Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        } else if (v instanceof CharSequence) {
            return v.toString().getBytes();
        } else {
            throw new IllegalArgumentException("Invalid type for byte[]: " + v + " of type " + v.getClass());
        }
    }
}
