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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.ssh.BashCommandsConfigurable;
import org.apache.brooklyn.util.ssh.IptablesCommandsConfigurable;

public class BrooklynOsCommands {

    public static final ConfigKey<Boolean> OS_COMMANDS_IGNORE_CERTS = ConfigKeys.newBooleanConfigKey("brooklyn.os.commands.ignoreCerts", "Whether to generate OS commands that ignore certs, e.g. curl -k");

    public static BashCommandsConfigurable bash(ManagementContext mgmt) {
        return BashCommandsConfigurable.newInstance().withIgnoreCerts( ((ManagementContextInternal)mgmt).getBrooklynProperties().getConfig(OS_COMMANDS_IGNORE_CERTS) );
    }

    public static IptablesCommandsConfigurable bashIptables(ManagementContext mgmt) {
        return new IptablesCommandsConfigurable(bash(mgmt));
    }

    public static BashCommandsConfigurable bash(Entity entity) {
        Boolean ignoreCerts = entity.config().get(OS_COMMANDS_IGNORE_CERTS);
        if (ignoreCerts!=null) return BashCommandsConfigurable.newInstance().withIgnoreCerts(ignoreCerts);
        return bash( ((EntityInternal)entity).getManagementContext() );
    }

    public static IptablesCommandsConfigurable bashIptables(Entity entity) {
        return new IptablesCommandsConfigurable(bash(entity));
    }

}
