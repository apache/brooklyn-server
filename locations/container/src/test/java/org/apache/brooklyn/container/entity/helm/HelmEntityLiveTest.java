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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertPredicateEventuallyTrue;

public class HelmEntityLiveTest extends BrooklynAppLiveTestSupport {

    @AfterMethod(alwaysRun = true, timeOut = Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    @Override
    public void tearDown() throws Exception {
        app.stop();
        super.tearDown();
    }

    @Test(groups = {"Live"})
    public void testSimpleDeploy() throws Exception {
        HelmEntity andManageChild = newHelmSpec("nginx-test", "bitnami/nginx");

        app.start(newKubernetesLocation());

        assertAttributeEqualsEventually(andManageChild, Attributes.SERVICE_UP, true);
        assertAttributeEqualsEventually(andManageChild, HelmEntity.DEPLOYMENT_READY, true);
    }

    @Test(groups = {"Live"})
    public void testMultiDeployment() {
        HelmEntity andManageChild = newHelmSpec("prometheus", "/Users/duncangrant/workspace/charts/stable/prometheus");

        app.start(newKubernetesLocation());

        assertPredicateEventuallyTrue(andManageChild, new Predicate<HelmEntity>() {
            @Override
            public boolean apply(@Nullable HelmEntity input) {
                List<String> status = input.getAttribute(HelmEntity.DEPLOYMENTS);
                return status != null && status.contains("prometheus-alertmanager");
            }
        });
    }


    @Test(groups = {"Live"})
    public void testCanSenseHelmStatus() {
        HelmEntity andManageChild = newHelmSpec("nginx-test", "bitnami/nginx");

        app.start(newKubernetesLocation());

        assertPredicateEventuallyTrue(andManageChild, new Predicate<HelmEntity>() {
            @Override
            public boolean apply(@Nullable HelmEntity input) {
                String status = input.getAttribute(HelmEntity.STATUS);
                return status == null? false : status.contains("STATUS: deployed");
            }
        });
    }

    @Test(groups = {"Live"})
    public void testCanSenseDeploymentStatus() {
        HelmEntity andManageChild = newHelmSpec("nginx-test", "bitnami/nginx");

        app.start(newKubernetesLocation());

        assertAttributeEqualsEventually(andManageChild, HelmEntity.DEPLOYMENT_READY, true);
    }

    @Test(groups = {"Live"})
    public void testCanScaleCluster() {
        HelmEntity andManageChild = newHelmSpec("nginx-test", "bitnami/nginx");

        app.start(newKubernetesLocation());

        assertAttributeEqualsEventually(andManageChild, HelmEntity.AVAILABLE_REPLICAS, 1);
        assertAttributeEqualsEventually(andManageChild, HelmEntity.REPLICAS, 1);

        andManageChild.resize("nginx-test",3);

        assertAttributeEqualsEventually(andManageChild, HelmEntity.AVAILABLE_REPLICAS, 3);
        assertAttributeEqualsEventually(andManageChild, HelmEntity.REPLICAS, 3);

        assertAttributeEqualsEventually(andManageChild, HelmEntity.DEPLOYMENT_READY, true);
    }

    @Test(groups = {"Live"})
    public void testCanScaleClusterPrometheus() {
        HelmEntity andManageChild = newHelmSpec("prometheus", "/Users/duncangrant/workspace/charts/stable/prometheus");

        app.start(newKubernetesLocation());

        assertAttributeEqualsEventually(andManageChild, HelmEntity.AVAILABLE_REPLICAS, 1);
        assertAttributeEqualsEventually(andManageChild, HelmEntity.REPLICAS, 1);

        andManageChild.resize("prometheus-server", 2);

        assertAttributeEqualsEventually(andManageChild, HelmEntity.AVAILABLE_REPLICAS, 2);
        assertAttributeEqualsEventually(andManageChild, HelmEntity.REPLICAS, 2);

        assertAttributeEqualsEventually(andManageChild, HelmEntity.DEPLOYMENT_READY, true);
    }

    private HelmEntity newHelmSpec(String templateInstallName, String helmTemplate) {
        return app.createAndManageChild(EntitySpec.create(HelmEntity.class)
                .configure(HelmEntity.REPO_NAME, "bitnami")
                .configure(HelmEntity.REPO_URL, "https://charts.bitnami.com/bitnami")
                .configure(HelmEntity.HELM_DEPLOYMENT_NAME, templateInstallName)
                .configure(HelmEntity.HELM_TEMPLATE, helmTemplate));
    }

    private ImmutableList<Location> newLocalhostLocation() {
        return ImmutableList.<Location>of(
                app.newLocalhostProvisioningLocation(
                        ImmutableMap.of(KubernetesLocation.KUBECONFIG, "/Users/duncangrant/.kube/config")));
    }

    private Collection<? extends Location> newKubernetesLocation() {
            Map<String, ?> allFlags = MutableMap.<String, Object>builder()
                    .put(KubernetesLocation.KUBECONFIG.getName(), "/Users/duncangrant/.kube/config")
                    .put("image", "cloudsoft/centos:7")
                    .build();
        KubernetesLocation kubernetesLocation = (KubernetesLocation) mgmt.getLocationRegistry().getLocationManaged("kubernetes", allFlags);
        return ImmutableList.of(kubernetesLocation);
    }
}