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
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Map;

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertPredicateEventuallyTrue;
import static org.testng.Assert.*;

public class HelmEntityLiveTest extends BrooklynAppLiveTestSupport {

    @Test
    public void testSimpleDeploy() throws Exception {
        HelmEntity andManageChild = app.createAndManageChild(EntitySpec.create(HelmEntity.class)
                .configure(HelmEntity.REPO_NAME, "bitnami")
                .configure(HelmEntity.REPO_URL, "https://charts.bitnami.com/bitnami")
                .configure(HelmEntity.HELM_TEMPLATE_INSTALL_NAME, "wordpress-test")
                .configure(HelmEntity.HELM_TEMPLATE, "bitnami/wordpress"));

        app.start(newLocalhostLocation());

        assertAttributeEqualsEventually(andManageChild, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertAttributeEqualsEventually(andManageChild, Attributes.SERVICE_UP, true);
        app.stop();
    }

    @Test
    public void testCanSenseHelmStatus() {
        HelmEntity andManageChild = app.createAndManageChild(EntitySpec.create(HelmEntity.class)
                .configure(HelmEntity.REPO_NAME, "bitnami")
                .configure(HelmEntity.REPO_URL, "https://charts.bitnami.com/bitnami")
                .configure(HelmEntity.HELM_TEMPLATE_INSTALL_NAME, "wordpress-test")
                .configure(HelmEntity.HELM_TEMPLATE, "bitnami/wordpress"));

        app.start(newLocalhostLocation());

        assertPredicateEventuallyTrue(andManageChild, new Predicate<HelmEntity>() {
            @Override
            public boolean apply(@Nullable HelmEntity input) {
                String status = input.getAttribute(HelmEntity.STATUS);
                return status == null? false : status.contains("STATUS: deployed");
            }
        });
        app.stop();
    }

    @Test
    public void testCanSenseDeploymentStatus() {
        HelmEntity andManageChild = app.createAndManageChild(EntitySpec.create(HelmEntity.class)
                .configure(HelmEntity.REPO_NAME, "bitnami")
                .configure(HelmEntity.REPO_URL, "https://charts.bitnami.com/bitnami")
                .configure(HelmEntity.HELM_TEMPLATE_INSTALL_NAME, "nginx-test")
                .configure(HelmEntity.HELM_TEMPLATE, "bitnami/nginx"));

        app.start(newLocalhostLocation());

        assertAttributeEqualsEventually(andManageChild, HelmEntity.DEPLOYMENT_READY, true);
        app.stop();
    }

    private ImmutableList<Location> newLocalhostLocation() {
        return ImmutableList.<Location>of(
                app.newLocalhostProvisioningLocation(
                        ImmutableMap.of(KubernetesLocation.KUBECONFIG, "/Users/duncangrant/.kube/config")));
    }
}