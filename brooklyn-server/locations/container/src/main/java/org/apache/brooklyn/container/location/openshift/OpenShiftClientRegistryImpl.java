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
package org.apache.brooklyn.container.location.openshift;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import org.apache.brooklyn.container.location.kubernetes.KubernetesClientRegistryImpl;
import org.apache.brooklyn.util.core.config.ConfigBag;

public class OpenShiftClientRegistryImpl extends KubernetesClientRegistryImpl {

    public static final OpenShiftClientRegistryImpl INSTANCE = new OpenShiftClientRegistryImpl();

    /**
     * The default OpenShift URL is set using the Kubernetes
     * {@code KubernetesLocationConfig#MASTER_URL master URL} as follows:
     * <pre>
     * openShiftUrl = URLUtils.join(getMasterUrl(), "oapi", oapiVersion);
     * </pre>
     */
    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        KubernetesClient client = super.getKubernetesClient(conf);
        OpenShiftClient oClient = client.adapt(OpenShiftClient.class);
        return oClient;
    }

}
