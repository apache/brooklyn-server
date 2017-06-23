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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.domain.Credentials;

import com.google.common.base.Supplier;

public class ComputeServiceRegistryImpl extends AbstractComputeServiceRegistry implements ComputeServiceRegistry, JcloudsLocationConfig {
    
    public static final ComputeServiceRegistryImpl INSTANCE = new ComputeServiceRegistryImpl();
        
    protected ComputeServiceRegistryImpl() {
    }

    @Override
    protected Supplier<Credentials> makeCredentials(ConfigBag conf) {
        String identity = checkNotNull(conf.get(CloudLocationConfig.ACCESS_IDENTITY), "identity must not be null");
        String credential = checkNotNull(conf.get(CloudLocationConfig.ACCESS_CREDENTIAL), "credential must not be null");
        return () -> new Credentials.Builder<>()
                .identity(identity)
                .credential(credential)
                .build();
    }

    @Override
    public String toString(){
        return getClass().getName();
    }

}
