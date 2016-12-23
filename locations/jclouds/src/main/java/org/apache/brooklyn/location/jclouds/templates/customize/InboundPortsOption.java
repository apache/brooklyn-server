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

import java.util.Arrays;
import java.util.Collections;

import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.commons.lang3.ArrayUtils;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

class InboundPortsOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(InboundPortsOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        int[] inboundPorts = toIntPortArray(v);
        if (LOG.isDebugEnabled())
            LOG.debug("opening inbound ports {} for cloud/type {}", Arrays.toString(inboundPorts), t.getClass());
        t.inboundPorts(inboundPorts);
    }

    private int[] toIntPortArray(Object v) {
        PortRange portRange = PortRanges.fromIterable(Collections.singletonList(v));
        return ArrayUtils.toPrimitive(Iterables.toArray(portRange, Integer.class));
    }
}
