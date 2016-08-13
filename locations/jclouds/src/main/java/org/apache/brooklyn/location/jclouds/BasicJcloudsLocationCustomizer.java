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

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.TemplateOptions;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;

/**
 * A default no-op implementation, which can be extended to override the appropriate methods.
 */
public class BasicJcloudsLocationCustomizer extends BasicConfigurableObject implements JcloudsLocationCustomizer {

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateBuilder templateBuilder) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, Template template) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
        // no-op
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        // no-op
    }

    @Override
    public void preRelease(JcloudsMachineLocation machine) {
        // no-op
    }

    @Override
    public void postRelease(JcloudsMachineLocation machine) {
        // no-op
    }

    /** @return the calling entity */
    protected Entity getCallerContext(JcloudsMachineLocation machine) {
        SudoTtyFixingCustomizer s;

        Object context = config().get(LocationConfigKeys.CALLER_CONTEXT);
        if (context == null) {
            context = machine.config().get(LocationConfigKeys.CALLER_CONTEXT);
        }
        if (!(context instanceof Entity)) {
            throw new IllegalStateException("Invalid location context: " + context);
        }
        Entity entity = (Entity) context;
        return entity;
    }
}
