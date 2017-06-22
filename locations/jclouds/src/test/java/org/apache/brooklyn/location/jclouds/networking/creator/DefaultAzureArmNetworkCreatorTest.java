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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.azurecompute.arm.AzureComputeApi;
import org.jclouds.azurecompute.arm.domain.ResourceGroup;
import org.jclouds.azurecompute.arm.domain.Subnet;
import org.jclouds.azurecompute.arm.features.ResourceGroupApi;
import org.jclouds.azurecompute.arm.features.SubnetApi;
import org.jclouds.azurecompute.arm.features.VirtualNetworkApi;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class DefaultAzureArmNetworkCreatorTest {

    @Mock ComputeService computeService;
    @Mock ComputeServiceContext computeServiceContext;
    @Mock AzureComputeApi azureComputeApi;
    @Mock ResourceGroupApi resourceGroupApi;
    @Mock VirtualNetworkApi virtualNetworkApi;
    @Mock SubnetApi subnetApi;

    @Mock ResourceGroup resourceGroup;
    @Mock Subnet subnet;

    final String TEST_LOCATION = "test-loc";
    final String TEST_RESOURCE_GROUP = "brooklyn-default-resource-group-" + TEST_LOCATION;
    final String TEST_NETWORK_NAME = "brooklyn-default-network-" + TEST_LOCATION;
    final String TEST_SUBNET_NAME = "brooklyn-default-subnet-" + TEST_LOCATION;
    final String TEST_SUBNET_ID = "/test/resource/id";

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        
        // stock mock responses
        when(computeService.getContext()).thenReturn(computeServiceContext);
        when(computeServiceContext.unwrapApi(AzureComputeApi.class)).thenReturn(azureComputeApi);
        when(azureComputeApi.getResourceGroupApi()).thenReturn(resourceGroupApi);
        when(azureComputeApi.getSubnetApi(TEST_RESOURCE_GROUP, TEST_NETWORK_NAME)).thenReturn(subnetApi);
        when(azureComputeApi.getVirtualNetworkApi(TEST_RESOURCE_GROUP)).thenReturn(virtualNetworkApi);
        when(subnet.id()).thenReturn(TEST_SUBNET_ID);
    }

    @Test
    public void testPreExisting() throws Exception {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);

        //Setup mocks
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(subnet);
        

        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify
        verify(subnetApi).get(TEST_SUBNET_NAME);
        verify(subnet).id();

        //verify templateOptions updated to include defaults
        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        Map<?, ?> ipOptions = (Map<?, ?>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
    }

    @Test
    public void testVanillaWhereNoResourceGroup() throws Exception {
        runVanilla(ImmutableMap.of());
    }
    
    @Test
    public void testVanillaWhereTemplateOptionsAlreadySpecified() throws Exception {
        ImmutableMap<?, ?> additionalConfig = ImmutableMap.of(TEMPLATE_OPTIONS, ImmutableMap.of("unrelated-key", "unrelated-value"));
        ConfigBag result = runVanilla(additionalConfig);
        Map<String, ?> templateOptions = result.get(TEMPLATE_OPTIONS);
        assertEquals(templateOptions.get("unrelated-key"), "unrelated-value");
    }

    protected ConfigBag runVanilla(Map<?, ?> additionalConfig) throws Exception {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.putAll(additionalConfig);
        
        //Setup mocks
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(null).thenReturn(subnet); //null first time, subnet next

        when(resourceGroupApi.get(TEST_RESOURCE_GROUP)).thenReturn(null);


        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify calls made
        verify(subnetApi, times(2)).get(TEST_SUBNET_NAME);
        verify(subnet).id();

        verify(resourceGroupApi).get(TEST_RESOURCE_GROUP);
        verify(resourceGroupApi).create(eq(TEST_RESOURCE_GROUP), eq(TEST_LOCATION), any());

        verify(virtualNetworkApi).createOrUpdate(eq(TEST_NETWORK_NAME), eq(TEST_LOCATION), any());

        //verify templateOptions updated to include defaults
        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        Map<?, ?> ipOptions = (Map<?, ?>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
        
        return configBag;
    }

    @Test
    public void testVanillaWhereExistingResourceGroup() throws Exception {
        //Setup config bag
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        
        //Setup mocks
        when(subnetApi.get(TEST_SUBNET_NAME)).thenReturn(null).thenReturn(subnet); //null first time, subnet next

        when(resourceGroupApi.get(TEST_RESOURCE_GROUP)).thenReturn(resourceGroup);


        //Test
        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //verify
        verify(subnetApi, times(2)).get(TEST_SUBNET_NAME);
        verify(subnet).id();

        verify(resourceGroupApi).get(TEST_RESOURCE_GROUP);
        verify(resourceGroupApi, never()).create(any(), any(), any());

        verify(virtualNetworkApi).createOrUpdate(eq(TEST_NETWORK_NAME), eq(TEST_LOCATION), any());

        //verify templateOptions updated to include defaults
        Map<String, Object> templateOptions = configBag.get(TEMPLATE_OPTIONS);
        Map<?, ?> ipOptions = (Map<?, ?>)templateOptions.get("ipOptions");
        assertEquals(ipOptions.get("subnet"), TEST_SUBNET_ID);
        assertEquals(ipOptions.get("allocateNewPublicIp"), true);
    }

    @Test
    public void testNetworkInConfig() throws Exception {
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(NETWORK_NAME, TEST_NETWORK_NAME);

        runAssertingNoIteractions(configBag);
    }

    @Test
    public void testNetworkInTemplate() throws Exception {
        HashMap<String, Object> templateOptions = new HashMap<>();
        templateOptions.put(NETWORK_NAME.getName(), TEST_NETWORK_NAME);

        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(TEMPLATE_OPTIONS, templateOptions);

        runAssertingNoIteractions(configBag);
    }

    @Test
    public void testIpOptionsInTemplate() throws Exception {
        HashMap<String, Object> templateOptions = new HashMap<>();
        templateOptions.put("ipOptions", TEST_NETWORK_NAME);

        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(TEMPLATE_OPTIONS, templateOptions);

        runAssertingNoIteractions(configBag);
    }

    @Test
    public void testConfigDisabled() throws Exception {
        ConfigBag configBag = ConfigBag.newInstance();
        configBag.put(CLOUD_REGION_ID, TEST_LOCATION);
        configBag.put(AZURE_ARM_DEFAULT_NETWORK_ENABLED, false);

        runAssertingNoIteractions(configBag);
    }
    
    protected void runAssertingNoIteractions(ConfigBag configBag) throws Exception {
        Map<String, Object> configCopy = configBag.getAllConfig();

        DefaultAzureArmNetworkCreator.createDefaultNetworkAndAddToTemplateOptionsIfRequired(computeService, configBag);

        //Ensure nothing changed, and no calls were made to the compute service
        assertEquals(configCopy, configBag.getAllConfig());
        Mockito.verifyZeroInteractions(computeService);
    }
}
