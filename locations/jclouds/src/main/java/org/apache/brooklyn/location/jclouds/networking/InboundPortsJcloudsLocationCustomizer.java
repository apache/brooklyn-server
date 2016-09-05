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
package org.apache.brooklyn.location.jclouds.networking;

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.jclouds.compute.ComputeService;
import org.jclouds.net.domain.IpPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.brooklyn.location.jclouds.networking.NetworkingEffectors.*;

/**
 * @deprecated Please use {@link org.apache.brooklyn.location.jclouds.networking.SharedLocationSecurityGroupCustomizer}
 */
@Beta
@Deprecated
public class InboundPortsJcloudsLocationCustomizer extends BasicJcloudsLocationCustomizer {
    public static final Logger LOG = LoggerFactory.getLogger(InboundPortsJcloudsLocationCustomizer.class);

    private String inboundPortsList;
    private String inboundPortsListProtocol;

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        Object callerContext = machine.getConfig(CloudLocationConfig.CALLER_CONTEXT);
        Entity entity;
        if (callerContext instanceof Entity) {
            entity = (Entity)callerContext;
        } else {
            throw new IllegalArgumentException("customizer should be called on against a jcloudsLocation which has callerContext of type Entity. Location " + location);
        }
        TaskAdaptable<Iterable<IpPermission>> taskAdaptable = Effectors.invocation(entity,
                NetworkingEffectors.OPEN_INBOUND_PORTS_IN_SECURITY_GROUP_EFFECTOR,
                MutableMap.<Object, Object>of(INBOUND_PORTS_LIST, inboundPortsList, INBOUND_PORTS_LIST_PROTOCOL, inboundPortsListProtocol, JCLOUDS_MACHINE_LOCATIN, machine));
        Iterable<IpPermission> result = DynamicTasks.submit(taskAdaptable.asTask(), entity).getUnchecked();
        LOG.info("Opened ports for " + entity + " " + result);
    }

    public void setInboundPortsList(String inboundPortsList) {
        this.inboundPortsList = inboundPortsList;
    }

    public void setInboundPortsListProtocol(String inboundPortsListProtocol) {
        this.inboundPortsListProtocol = inboundPortsListProtocol;
    }
}
