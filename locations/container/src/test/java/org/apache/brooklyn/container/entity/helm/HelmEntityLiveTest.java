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
package org.apache.brooklyn.container.entity.helm;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.testng.Assert.*;

public class HelmEntityLiveTest extends BrooklynAppLiveTestSupport {

    @Test
    public void testSimpleDeploy() throws Exception {
        HelmEntity andManageChild = app.createAndManageChild(EntitySpec.create(HelmEntity.class)
                .configure(HelmEntity.REPO_NAME, "bitnami")
                .configure(HelmEntity.REPO_URL, "https://charts.bitnami.com/bitnami")
                .configure(HelmEntity.HELM_TEMPLATE_INSTALL_NAME, "wordpress-test")
                .configure(HelmEntity.HELM_TEMPLATE, "bitnami/wordpress"));

        app.start(ImmutableList.<Location>of(app.newLocalhostProvisioningLocation()));

        assertAttributeEqualsEventually(andManageChild, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        app.stop();
    }

    protected KubernetesLocation newKubernetesLocation(Map<String, ?> flags) throws Exception {
        Map<String, ?> allFlags = MutableMap.<String, Object>builder()
                .put("kubeconfig", "/Users/duncangrant/.kube/config")
                .put("image", "cloudsoft/centos:7")
                .put("loginUser", "root")
                .put("loginUser.password", "p4ssw0rd")
                .putAll(flags)
                .build();
        return (KubernetesLocation) mgmt.getLocationRegistry().getLocationManaged("kubernetes", allFlags);
    }
}