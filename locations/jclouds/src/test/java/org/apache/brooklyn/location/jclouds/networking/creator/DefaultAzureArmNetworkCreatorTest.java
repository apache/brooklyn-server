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
import static org.apache.brooklyn.location.jclouds.networking.creator.DefaultAzureArmNetworkCreator.AZURE_ARM_DEFAULT_NETWORK_ENABLED;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import org.jclouds.azurecompute.arm.AzureComputeApi;
import org.jclouds.azurecompute.arm.domain.Subnet;
import org.jclouds.azurecompute.arm.features.ResourceGroupApi;
import org.jclouds.azurecompute.arm.features.SubnetApi;
import org.jclouds.azurecompute.arm.features.VirtualNetworkApi;
import org.jclouds.compute.ComputeService;

import org.apache.brooklyn.util.core.config.ConfigBag;

public class DefaultAzureArmNetworkCreatorTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS) ComputeService computeService;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) AzureComputeApi azureComputeApi;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) ResourceGroupApi resourceGroupApi;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) VirtualNetworkApi virtualNetworkApi;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) SubnetApi subnetApi;

    @Mock Subnet subnet;

    final String TEST_RESOURCE_GROUP = "brooklyn-default-resource-group-test-loc";
    final String TEST_NETWORK_NAME = "brooklyn-default-network-test-loc";
    final String TEST_SUBNET_NAME = "brooklyn-default-subnet-test-loc";
    final String TEST_SUBNET_ID = "/test/resource/id";
    final String TEST_LOCATION = "test-loc";

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testPreExisting() {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);

        //Setup mocks
        when(computeService.getContext().unwrapApi(AzureComputeApi.class)).thenReturn(azureComputeApi);
        when(azureComputeApi.getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME)).thenReturn(subnetApi);
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(subnet);
        when(subnet.id()).thenReturn(TEST_SUBNET_ID);

        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify
        verify(subnetApi).get(TEST_SUBNET_NAME);
        verify(subnet).id();
        verify(azureComputeApi).getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME);

        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        Map<String, Object> ipOptions = (Map<String, Object>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
    }

    @Test
    public void testVanilla() {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);

        //Setup mocks
        when(computeService.getContext().unwrapApi(AzureComputeApi.class)).thenReturn(azureComputeApi);
        when(azureComputeApi.getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME)).thenReturn(subnetApi);
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(null).thenReturn(subnet); //null first time, subnet next
        when(subnet.id()).thenReturn(TEST_SUBNET_ID);

        when(azureComputeApi.getResourceGroupApi()).thenReturn(resourceGroupApi);
        when(resourceGroupApi.get(TEST_RESOURCE_GROUP)).thenReturn(null);

        when(azureComputeApi.getVirtualNetworkApi(TEST_RESOURCE_GROUP)).thenReturn(virtualNetworkApi);


        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify
        verify(subnetApi, times(2)).get(TEST_SUBNET_NAME);
        verify(subnet).id();
        verify(azureComputeApi, times(2)).getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME);

        verify(azureComputeApi, times(2)).getResourceGroupApi();
        verify(resourceGroupApi).get(TEST_RESOURCE_GROUP);
        verify(azureComputeApi).getVirtualNetworkApi(TEST_RESOURCE_GROUP);

        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        Map<String, Object> ipOptions = (Map<String, Object>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
    }

    @Test
    public void testVanillaWhereTemplateOptionsAlreadySpecified() {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(TEMPLATE_OPTIONS, ImmutableMap.of("unrelated-key", "unrelated-value"));

        //Setup mocks
        when(computeService.getContext().unwrapApi(AzureComputeApi.class)).thenReturn(azureComputeApi);
        when(azureComputeApi.getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME)).thenReturn(subnetApi);
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(null).thenReturn(subnet); //null first time, subnet next
        when(subnet.id()).thenReturn(TEST_SUBNET_ID);

        when(azureComputeApi.getResourceGroupApi()).thenReturn(resourceGroupApi);
        when(resourceGroupApi.get(TEST_RESOURCE_GROUP)).thenReturn(null);

        when(azureComputeApi.getVirtualNetworkApi(TEST_RESOURCE_GROUP)).thenReturn(virtualNetworkApi);


        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify
        verify(subnetApi, times(2)).get(TEST_SUBNET_NAME);
        verify(subnet).id();
        verify(azureComputeApi, times(2)).getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME);

        verify(azureComputeApi, times(2)).getResourceGroupApi();
        verify(resourceGroupApi).get(TEST_RESOURCE_GROUP);
        verify(azureComputeApi).getVirtualNetworkApi(TEST_RESOURCE_GROUP);

        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        assertEquals(templateOptions.get("unrelated-key"), "unrelated-value");

        Map<String, Object> ipOptions = (Map<String, Object>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
    }

    @Test
    public void testNetworkInConfig() {
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(NETWORK_NAME, TEST_NETWORK_NAME);

        Map<String, Object> configCopy = configBag.getAllConfig();

        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //Ensure nothing changed, and no calls were made to the compute service
        assertEquals(configCopy, configBag.getAllConfig());
        Mockito.verifyZeroInteractions(computeService);
    }

    @Test
    public void testNetworkInTemplate() {
        HashMap<String, Object> templateOptions = new HashMap<>();
        templateOptions.put(NETWORK_NAME.getName(), TEST_NETWORK_NAME);

        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(TEMPLATE_OPTIONS, templateOptions);

        Map<String, Object> configCopy = configBag.getAllConfig();

        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //Ensure nothing changed, and no calls were made to the compute service
        assertEquals(configCopy, configBag.getAllConfig());
        Mockito.verifyZeroInteractions(computeService);
    }

    @Test
    public void testIpOptionsInTemplate() {
        HashMap<String, Object> templateOptions = new HashMap<>();
        templateOptions.put("ipOptions", TEST_NETWORK_NAME);

        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(TEMPLATE_OPTIONS, templateOptions);

        Map<String, Object> configCopy = configBag.getAllConfig();

        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //Ensure nothing changed, and no calls were made to the compute service
        assertEquals(configCopy, configBag.getAllConfig());
        Mockito.verifyZeroInteractions(computeService);
    }

    @Test
    public void testConfigDisabled() {
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(AZURE_ARM_DEFAULT_NETWORK_ENABLED, false);

        Map<String, Object> configCopy = configBag.getAllConfig();

        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //Ensure nothing changed, and no calls were made to the compute service
        assertEquals(configCopy, configBag.getAllConfig());
        Mockito.verifyZeroInteractions(computeService);
    }
}
