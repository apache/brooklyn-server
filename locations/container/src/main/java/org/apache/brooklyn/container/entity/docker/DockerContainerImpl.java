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
package org.apache.brooklyn.container.entity.docker;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.network.OnPublicNetworkEnricher;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessImpl;
import org.apache.brooklyn.util.collections.MutableList;

import java.util.Iterator;
import java.util.List;

public class DockerContainerImpl extends EmptySoftwareProcessImpl implements DockerContainer {

    @Override
    public void init() {
        super.init();

        String imageName = config().get(DockerContainer.IMAGE_NAME);
        if (!Strings.isNullOrEmpty(imageName)) {
            config().set(PROVISIONING_PROPERTIES.subKey("imageId"), imageName);
        }

        if (Boolean.TRUE.equals(config().get(DockerContainer.DISABLE_SSH))) {
            config().set(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true);
            config().set(PROVISIONING_PROPERTIES.subKey("useJcloudsSshInit"), false);
            config().set(PROVISIONING_PROPERTIES.subKey("waitForSshable"), false);
            config().set(PROVISIONING_PROPERTIES.subKey("pollForFirstReachableAddress"), false);
            config().set(EmptySoftwareProcessImpl.USE_SSH_MONITORING, false);
        }

        ImmutableSet.Builder<AttributeSensor<Integer>> builder = ImmutableSet.builder();
        List<String> portRanges = MutableList.copyOf(config().get(DockerContainer.INBOUND_TCP_PORTS));
        for (String portRange : portRanges) {
            Iterator<Integer> iterator = PortRanges.fromString(portRange).iterator();
            while (iterator.hasNext()) {
                Integer port = iterator.next();
                AttributeSensor<Integer> element = Sensors.newIntegerSensor("docker.port." + port);
                sensors().set(element, port);
                builder.add(element);
            }
        }

        enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class).configure(OnPublicNetworkEnricher.SENSORS, builder.build()));
    }

    @Override
    protected void disconnectSensors() {
        if (isSshMonitoringEnabled()) {
            disconnectServiceUpIsRunning();
        }
        super.disconnectSensors();
    }

    @Override
    protected void connectSensors() {
        super.connectSensors();
        if (isSshMonitoringEnabled()) {
            connectServiceUpIsRunning();
        } else {
            sensors().set(Attributes.SERVICE_UP, true);
        }
    }

}
