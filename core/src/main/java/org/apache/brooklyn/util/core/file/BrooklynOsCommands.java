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
package org.apache.brooklyn.util.core.file;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.ssh.BashCommandsConfigurable;
import org.apache.brooklyn.util.ssh.IptablesCommandsConfigurable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BrooklynOsCommands {

    public static final ConfigKey<Boolean> SSH_CONFIG_SCRIPT_IGNORE_CERTS = BrooklynConfigKeys.SSH_CONFIG_SCRIPTS_IGNORE_CERTS;
    public static final ConfigKey<Boolean> SCRIPT_IGNORE_CERTS = ConfigKeys.newConfigKeyWithPrefixRemoved(BrooklynConfigKeys.BROOKLYN_SSH_CONFIG_KEY_PREFIX, SSH_CONFIG_SCRIPT_IGNORE_CERTS);

    public static BashCommandsConfigurable bash(ManagementContext mgmt) {
        return BashCommandsConfigurable.newInstance().withIgnoreCerts( ((ManagementContextInternal)mgmt).getBrooklynProperties().getConfig(SSH_CONFIG_SCRIPT_IGNORE_CERTS) );
    }

    public static IptablesCommandsConfigurable bashIptables(ManagementContext mgmt) {
        return new IptablesCommandsConfigurable(bash(mgmt));
    }

    public static BashCommandsConfigurable bash(Entity entity, boolean includeMachineLocations) {
        return bashForBrooklynObjects(includeMachineLocations, entity);
    }

    public static BashCommandsConfigurable bash(Location location) {
        return bashForBrooklynObjects(false, location);
    }

    public static BashCommandsConfigurable bashForBrooklynObjects(boolean includeMachineLocations, BrooklynObject ...brooklynObjects) {
        return bashForBrooklynObjects(includeMachineLocations, Arrays.asList(brooklynObjects));
    }
    public static BashCommandsConfigurable bashForBrooklynObjects(boolean includeMachineLocations, List<BrooklynObject> brooklynObjects0) {
        ManagementContext mgmt = null;
        List<BrooklynObject> brooklynObjects = MutableList.of();
        for (BrooklynObject bo: brooklynObjects0) {
            if (includeMachineLocations && bo instanceof Entity) brooklynObjects.addAll( ((Entity)bo).getLocations().stream().filter(l -> l instanceof MachineLocation).collect(Collectors.toList()) );

            brooklynObjects.add(bo);
        }

        for (BrooklynObject bo: brooklynObjects) {
            Boolean ignoreCerts = null;
            // unprefixed key allowed for locations
            if (bo instanceof Location) ignoreCerts = bo.config().get(SCRIPT_IGNORE_CERTS);

            if (ignoreCerts==null) ignoreCerts = bo.config().get(SSH_CONFIG_SCRIPT_IGNORE_CERTS);
            if (ignoreCerts!=null) return BashCommandsConfigurable.newInstance().withIgnoreCerts(ignoreCerts);

            if (mgmt==null) mgmt = ((BrooklynObjectInternal)bo).getManagementContext();
        }

        return bash(mgmt);
    }

    public static IptablesCommandsConfigurable bashIptables(BrooklynObject entityOrOtherBrooklynObject) {
        return new IptablesCommandsConfigurable(bashForBrooklynObjects(true, entityOrOtherBrooklynObject));
    }

}
