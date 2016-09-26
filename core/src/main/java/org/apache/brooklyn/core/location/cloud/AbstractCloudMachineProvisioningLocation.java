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
package org.apache.brooklyn.core.location.cloud;

import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public abstract class AbstractCloudMachineProvisioningLocation extends AbstractLocation
        implements MachineProvisioningLocation<MachineLocation>, CloudLocationConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCloudMachineProvisioningLocation.class);

    public AbstractCloudMachineProvisioningLocation() {
        super();
    }

    /** typically wants at least ACCESS_IDENTITY and ACCESS_CREDENTIAL */
    public AbstractCloudMachineProvisioningLocation(Map<?,?> conf) {
        super(conf);
    }

    /** uses reflection to create an object of the same type, assuming a Map constructor;
     * subclasses can extend and downcast the result */
    @Override
    public AbstractCloudMachineProvisioningLocation newSubLocation(Map<?,?> newFlags) {
        return newSubLocation(getClass(), newFlags);
    }

    public AbstractCloudMachineProvisioningLocation newSubLocation(Class<? extends AbstractCloudMachineProvisioningLocation> type, Map<?,?> newFlags) {
        // TODO should be able to use ConfigBag.newInstanceExtending; would require moving stuff around to api etc
        // TODO was previously `return LocationCreationUtils.newSubLocation(newFlags, this)`; need to retest on CloudStack etc
        return getManagementContext().getLocationManager().createLocation(LocationSpec.create(type)
                .parent(this)
                .configure(config().getLocalBag().getAllConfig()) // FIXME Should this just be inherited?
                .configure(newFlags));
    }
    
    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        if (tags.size() > 0) {
            LOG.warn("Location {}, ignoring provisioning tags {}", this, tags);
        }
        return MutableMap.<String, Object>of();
    }

    // ---------------- utilities --------------------
    
    protected ConfigBag extractSshConfig(ConfigBag setup, ConfigBag alt) {
        ConfigBag sshConfig = new ConfigBag();
        
        for (HasConfigKey<?> key : SshMachineLocation.ALL_SSH_CONFIG_KEYS) {
            String keyName = key.getConfigKey().getName();
            if (setup.containsKey(keyName)) {
                sshConfig.putStringKey(keyName, setup.getStringKey(keyName));
            } else if (alt.containsKey(keyName)) {
                sshConfig.putStringKey(keyName, setup.getStringKey(keyName));
            }
        }
        
        Map<String, Object> sshToolClassProperties = Maps.filterKeys(setup.getAllConfig(), StringPredicates.startsWith(SshMachineLocation.SSH_TOOL_CLASS_PROPERTIES_PREFIX));
        sshConfig.putAll(sshToolClassProperties);

        // Special cases (preserving old code!)
        if (setup.containsKey(PASSWORD)) {
            sshConfig.copyKeyAs(setup, PASSWORD, SshTool.PROP_PASSWORD);
        } else if (alt.containsKey(PASSWORD)) {
            sshConfig.copyKeyAs(alt, PASSWORD, SshTool.PROP_PASSWORD);
        }
        
        if (setup.containsKey(PRIVATE_KEY_DATA)) {
            sshConfig.copyKeyAs(setup, PRIVATE_KEY_DATA, SshTool.PROP_PRIVATE_KEY_DATA);
        } else if (setup.containsKey(PRIVATE_KEY_FILE)) {
            sshConfig.copyKeyAs(setup, PRIVATE_KEY_FILE, SshTool.PROP_PRIVATE_KEY_FILE);
        } else if (alt.containsKey(PRIVATE_KEY_DATA)) {
            sshConfig.copyKeyAs(setup, PRIVATE_KEY_DATA, SshTool.PROP_PRIVATE_KEY_DATA);
        }
        
        if (setup.containsKey(PRIVATE_KEY_PASSPHRASE)) {
            // NB: not supported in jclouds (but it is by our ssh tool)
            sshConfig.copyKeyAs(setup, PRIVATE_KEY_PASSPHRASE, SshTool.PROP_PRIVATE_KEY_PASSPHRASE);
        }

        return sshConfig;
    }
}
