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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.net.URI;

import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.internal.BaseApiMetadata;
import org.jclouds.aws.ec2.AWSEC2ApiMetadata;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.internal.BaseProviderMetadata;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class JcloudsProviderAndApiLoaderTest {

    @Test
    public void testGetProvider() throws Exception {
        assertIsProvider("aws-ec2");
    }
    
    @Test
    public void testGetApi() throws Exception {
        assertIsApi("ec2");
    }

    @Test
    public void testRegisteredProvider() throws Exception {
        String id = "my-example-provider";
        assertFalse(JcloudsProviderAndApiLoader.isProvider(id));
        
        ProviderMetadata provider = new BaseProviderMetadata.Builder()
                .id(id)
                .name("My Example Provider")
                .apiMetadata(new AWSEC2ApiMetadata())
                .build();
        ProviderRegistry.registerProvider(provider);
        try {
            assertIsProvider(id);
            
            ProviderRegistry.unregisterProvider(provider);
            assertFalse(JcloudsProviderAndApiLoader.isProvider(id));
            
        } finally {
            ProviderRegistry.unregisterProvider(provider);
        }
    }

    @Test
    public void testRenamedRegisteredProvider() throws Exception {
        String newId = "my-example-provider2";
        String oldId = "my-example-provider-renamed";
        assertFalse(JcloudsProviderAndApiLoader.isProvider(newId));

        ProviderMetadata provider = new BaseProviderMetadata.Builder()
                .id(newId)
                .name("My Example Provider 2")
                .apiMetadata(new AWSEC2ApiMetadata())
                .build();
        ProviderRegistry.registerProvider(provider);
        try {
            assertIsProvider(newId);
            assertFalse(JcloudsProviderAndApiLoader.isProvider(oldId));

            DeserializingJcloudsRenamesProvider.INSTANCE.loadDeserializingMapping().put(oldId,newId);

            assertIsProvider(oldId, newId);

            ProviderRegistry.unregisterProvider(provider);
            assertFalse(JcloudsProviderAndApiLoader.isProvider(newId));
            assertFalse(JcloudsProviderAndApiLoader.isProvider(oldId));

        } finally {
            ProviderRegistry.unregisterProvider(provider);
        }
    }
    
    @Test
    public void testRegisteredApi() throws Exception {
        String id = "my-example-api";
        assertFalse(JcloudsProviderAndApiLoader.isApi(id));
        
        ApiMetadata api = new MyExampleApiMetadata.Builder()
                .id(id)
                .name("My Example API")
                .identityName("myIdName")
                .documentation(URI.create("http://myexampleapi/docs"))
                .build();
        ApiRegistry.registerApi(api);
        try {
            assertIsApi(id);
            
            ApiRegistry.unRegisterApi(api);
            assertFalse(JcloudsProviderAndApiLoader.isApi(id));
            
        } finally {
            ApiRegistry.unRegisterApi(api);
        }
    }
    
    private void assertIsProvider(String id) {
        assertIsProvider(id, id);
    }

    private void assertIsProvider(String id, String expectedId) {
        Optional<ProviderMetadata> result = JcloudsProviderAndApiLoader.getProvider(id);
        assertTrue(result.isPresent());
        assertEquals(result.get().getId(), expectedId);

        Optional<ApiMetadata> result2 = JcloudsProviderAndApiLoader.getApi(id);
        assertFalse(result2.isPresent(), "result="+result2);

        assertTrue(JcloudsProviderAndApiLoader.isProvider(id));
        assertFalse(JcloudsProviderAndApiLoader.isApi(id));
    }

    private void assertIsApi(String id) {
        Optional<ApiMetadata> result = JcloudsProviderAndApiLoader.getApi(id);
        assertTrue(result.isPresent());
        assertEquals(result.get().getId(), id);
        
        Optional<ProviderMetadata> result2 = JcloudsProviderAndApiLoader.getProvider(id);
        assertFalse(result2.isPresent(), "result="+result2);
        
        assertTrue(JcloudsProviderAndApiLoader.isApi(id));
        assertFalse(JcloudsProviderAndApiLoader.isProvider(id));
    }
    
    static class MyExampleApiMetadata extends BaseApiMetadata {
        public MyExampleApiMetadata() {
            super(new Builder());
        }
        
        public MyExampleApiMetadata(Builder builder) {
            super(builder);
        }
        
        static class Builder extends BaseApiMetadata.Builder<Builder> {
            protected Builder self() {
                return this;
            }
            @Override
            public MyExampleApiMetadata build() {
                return new MyExampleApiMetadata(this);
            }
        }
        
        @Override public org.jclouds.apis.ApiMetadata.Builder<?> toBuilder() {
            throw new UnsupportedOperationException();
        }
    }
}
