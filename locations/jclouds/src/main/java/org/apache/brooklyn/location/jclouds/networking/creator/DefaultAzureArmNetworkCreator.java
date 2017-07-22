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
package org.apache.brooklyn.location.jclouds.networking.creator;

import static org.apache.brooklyn.core.location.cloud.CloudLocationConfig.CLOUD_REGION_ID;
import static org.apache.brooklyn.location.jclouds.api.JcloudsLocationConfigPublic.NETWORK_NAME;
import static org.apache.brooklyn.location.jclouds.api.JcloudsLocationConfigPublic.TEMPLATE_OPTIONS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jclouds.azurecompute.arm.AzureComputeApi;
import org.jclouds.azurecompute.arm.domain.ResourceGroup;
import org.jclouds.azurecompute.arm.domain.Subnet;
import org.jclouds.azurecompute.arm.domain.VirtualNetwork;
import org.jclouds.azurecompute.arm.features.SubnetApi;
import org.jclouds.azurecompute.arm.features.VirtualNetworkApi;
import org.jclouds.compute.ComputeService;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;

public class DefaultAzureArmNetworkCreator {

    public static final Logger LOG = LoggerFactory.getLogger(DefaultAzureArmNetworkCreator.class);

    private static final String DEFAULT_RESOURCE_GROUP_PREFIX = "brooklyn-default-resource-group";
    private static final String DEFAULT_NETWORK_NAME_PREFIX = "brooklyn-default-network";
    private static final String DEFAULT_SUBNET_NAME_PREFIX = "brooklyn-default-subnet";

    private static final String PROVISIONING_STATE_UPDATING = "Updating";
    private static final String PROVISIONING_STATE_SUCCEEDED = "Succeeded";

    private static final String DEFAULT_VNET_ADDRESS_PREFIX = "10.1.0.0/16";
    private static final String DEFAULT_SUBNET_ADDRESS_PREFIX = "10.1.0.0/24";

    public static ConfigKey<Boolean> AZURE_ARM_DEFAULT_NETWORK_ENABLED = ConfigKeys.newBooleanConfigKey(
            "azure.arm.default.network.enabled",
            "When set to true, AMP will create a default network and subnet per Azure region and " +
                    "deploy applications there (if no network configuration has been set for the application).",
            true);

    public static void createDefaultNetworkAndAddToTemplateOptionsIfRequired(ComputeService computeService, ConfigBag config) {
        if (!config.get(AZURE_ARM_DEFAULT_NETWORK_ENABLED)) {
            LOG.debug("azure.arm.default.network.enabled is disabled, not creating default network");
            return;
        }
        String location = config.get(CLOUD_REGION_ID);
        if(StringUtils.isEmpty(location)) {
            LOG.debug("No region information, so cannot create a default network");
            return;
        }

        Map<String, Object> templateOptions = config.get(TEMPLATE_OPTIONS);


        //Only create a default network if we haven't specified a network name (in template options or config) or ip options
        if (config.containsKey(NETWORK_NAME)) {
            LOG.debug("Network config [{}] specified when provisioning Azure machine. Not creating default network", NETWORK_NAME.getName());
            return;
        }
        if (templateOptions != null && (templateOptions.containsKey("networks") || templateOptions.containsKey("ipOptions"))) {
            LOG.debug("Network config specified when provisioning Azure machine. Not creating default network");
            return;
        }

        AzureComputeApi api = computeService.getContext().unwrapApi(AzureComputeApi.class);

        String resourceGroupName = DEFAULT_RESOURCE_GROUP_PREFIX  + "-" + location;
        String vnetName = DEFAULT_NETWORK_NAME_PREFIX + "-" + location;
        String subnetName = DEFAULT_SUBNET_NAME_PREFIX + "-" + location;

        SubnetApi subnetApi = api.getSubnetApi(resourceGroupName, vnetName);
        VirtualNetworkApi virtualNetworkApi = api.getVirtualNetworkApi(resourceGroupName);

        //Check if default already exists
        Subnet preexistingSubnet = subnetApi.get(subnetName);
        if(preexistingSubnet != null){
            LOG.info("Using pre-existing default Azure network [{}] and subnet [{}] when provisioning machine", 
                    vnetName, subnetName);
            updateTemplateOptions(config, preexistingSubnet);
            return;
        }

        createResourceGroupIfNeeded(api, resourceGroupName, location);

        Subnet.SubnetProperties subnetProperties = Subnet.SubnetProperties.builder().addressPrefix(DEFAULT_SUBNET_ADDRESS_PREFIX).build();

        //Creating network + subnet if network doesn't exist
        if(virtualNetworkApi.get(vnetName) == null) {
            LOG.info("Network config not specified when provisioning Azure machine, and default network/subnet does not exists. "
                    + "Creating network [{}] and subnet [{}], and updating template options", vnetName, subnetName);

            Subnet subnet = Subnet.create(subnetName, null, null, subnetProperties);

            VirtualNetwork.VirtualNetworkProperties virtualNetworkProperties = VirtualNetwork.VirtualNetworkProperties
                    .builder().addressSpace(VirtualNetwork.AddressSpace.create(Arrays.asList(DEFAULT_VNET_ADDRESS_PREFIX)))
                    .subnets(Arrays.asList(subnet)).build();
            virtualNetworkApi.createOrUpdate(vnetName, location, virtualNetworkProperties);
        } else {
            LOG.info("Network config not specified when provisioning Azure machine, and default subnet does not exists. "
                    + "Creating subnet [{}] on network [{}], and updating template options", subnetName, vnetName);

            //Just creating the subnet
            subnetApi.createOrUpdate(subnetName, subnetProperties);
        }

        //Get created subnet
        Subnet createdSubnet = api.getSubnetApi(resourceGroupName, vnetName).get(subnetName);

        //Wait until subnet is created
        CountdownTimer timeout = CountdownTimer.newInstanceStarted(Duration.minutes(new Integer(20)));
        while (createdSubnet == null || createdSubnet.properties()  == null || PROVISIONING_STATE_UPDATING.equals(createdSubnet.properties().provisioningState())) {
            if (timeout.isExpired()) {
                throw new IllegalStateException("Creating subnet " + subnetName + " stuck in the updating state, aborting.");
            }
            LOG.debug("Created subnet {} is still in updating state, waiting for it to complete", createdSubnet);
            Duration.sleep(Duration.ONE_SECOND);
            createdSubnet = api.getSubnetApi(resourceGroupName, vnetName).get(subnetName);
        }

        String lastProvisioningState = createdSubnet.properties().provisioningState();
        if (!lastProvisioningState.equals(PROVISIONING_STATE_SUCCEEDED)) {
            LOG.debug("Created subnet {} in wrong state, expected state {} but found {}", new Object[] {subnetName, PROVISIONING_STATE_SUCCEEDED, lastProvisioningState});
            throw new IllegalStateException("Created subnet " + subnetName + " in wrong state, expected state " + PROVISIONING_STATE_SUCCEEDED +
                    " but found " + lastProvisioningState );
        }

        //Add config
        updateTemplateOptions(config, createdSubnet);
    }

    private static void updateTemplateOptions(ConfigBag config, Subnet createdSubnet){
        Map<String, Object> templateOptions;

        if(config.containsKey(TEMPLATE_OPTIONS)) {
            templateOptions = MutableMap.copyOf(config.get(TEMPLATE_OPTIONS));
        } else {
            templateOptions = new HashMap<>();
        }

        templateOptions.put("ipOptions", ImmutableList.of(ImmutableMap.of(
                "allocateNewPublicIp", true, //jclouds will not provide a public IP unless we set this
                "subnet", createdSubnet.id()
        )));

        config.put(TEMPLATE_OPTIONS, templateOptions);

    }

    private static void createResourceGroupIfNeeded(AzureComputeApi api, String resourceGroup, String location) {
        ResourceGroup rg = api.getResourceGroupApi().get(resourceGroup);
        if (rg == null) {
            LOG.info("Default Azure resource group [{}] does not exist in {}. Creating!", resourceGroup, location);
            api.getResourceGroupApi().create(resourceGroup, location,
                    ImmutableMap.of("description", "brooklyn default resource group"));
        } else {
            LOG.debug("Using existing default Azure resource group [{}] in {}", resourceGroup, location);
        }
    }
}
