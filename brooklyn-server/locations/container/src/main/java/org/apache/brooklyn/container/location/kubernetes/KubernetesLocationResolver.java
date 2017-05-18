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
package org.apache.brooklyn.container.location.kubernetes;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locations starting with the given prefix (@code "kubernetes") will use this resolver, to instantiate
 * a {@link KubernetesLocation}.
 * <p>
 * We ensure that config will be picked up from brooklyn.properties using the appropriate precedence:
 * <ol>
 * <li>named location config
 * <li>Prefix {@code brooklyn.location.kubernetes.}
 * <li>Prefix {@code brooklyn.kubernetes.}
 * </ol>
 */
public class KubernetesLocationResolver extends AbstractLocationResolver implements LocationResolver {

    public static final Logger log = LoggerFactory.getLogger(KubernetesLocationResolver.class);

    public static final String PREFIX = "kubernetes";

    @Override
    public boolean isEnabled() {
        return LocationConfigUtils.isResolverPrefixEnabled(managementContext, getPrefix());
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    protected Class<? extends Location> getLocationType() {
        return KubernetesLocation.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new SpecParser(getPrefix()).setExampleUsage("\"kubernetes\"");
    }

}
