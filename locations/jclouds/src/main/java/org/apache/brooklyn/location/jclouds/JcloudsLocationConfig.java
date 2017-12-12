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

import java.util.Collection;

import com.google.inject.Module;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.location.jclouds.api.JcloudsLocationConfigPublic;
import org.apache.brooklyn.location.jclouds.domain.JcloudsContext;
import org.apache.brooklyn.location.jclouds.networking.JcloudsPortForwarderExtension;
import org.jclouds.Context;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.domain.LoginCredentials;

import com.google.common.base.Function;
import com.google.common.reflect.TypeToken;

public interface JcloudsLocationConfig extends JcloudsLocationConfigPublic {

    public static final ConfigKey<LoginCredentials> CUSTOM_CREDENTIALS = new BasicConfigKey<LoginCredentials>(LoginCredentials.class,
        "customCredentials", "Custom jclouds LoginCredentials object to be used to connect to the VM", null);

    public static final ConfigKey<TemplateBuilder> TEMPLATE_BUILDER = ConfigKeys.newConfigKey(TemplateBuilder.class,
        "templateBuilder", "A TemplateBuilder instance provided programmatically, to be used when creating a VM");

    /** @deprecated since 0.7.0; use {@link #JCLOUDS_LOCATION_CUSTOMIZERS} instead */
    @Deprecated
    public static final ConfigKey<JcloudsLocationCustomizer> JCLOUDS_LOCATION_CUSTOMIZER = ConfigKeys.newConfigKey(JcloudsLocationCustomizer.class,
            "customizer", "Optional location customizer");

    @SuppressWarnings("serial")
    public static final ConfigKey<Collection<JcloudsLocationCustomizer>> JCLOUDS_LOCATION_CUSTOMIZERS = ConfigKeys.newConfigKey(
            new TypeToken<Collection<JcloudsLocationCustomizer>>() {},
            "customizers", "Optional location customizers");

    /** @deprecated since 0.7.0; use {@link #JCLOUDS_LOCATION_CUSTOMIZERS} instead */
    @Deprecated
    public static final ConfigKey<String> JCLOUDS_LOCATION_CUSTOMIZER_TYPE = ConfigKeys.newStringConfigKey(
            "customizerType", "Optional location customizer type (to be class-loaded and constructed with either a ConfigBag or no-arg constructor)");

    /** @deprecated since 0.7.0; use {@link #JCLOUDS_LOCATION_CUSTOMIZERS} instead */
    @Deprecated
    public static final ConfigKey<String> JCLOUDS_LOCATION_CUSTOMIZERS_SUPPLIER_TYPE = ConfigKeys.newStringConfigKey(
            "customizersSupplierType", "Optional type of a Supplier<Collection<JcloudsLocationCustomizer>> " +
            "(to be class-loaded and constructed with either a ConfigBag or no-arg constructor)");

    ConfigKey<ConnectivityResolver> CONNECTIVITY_RESOLVER = ConfigKeys.newConfigKey(ConnectivityResolver.class,
            "connectivityResolver",
            "Optional instance of a ConnectivityResolver that the location will use in favour of " + DefaultConnectivityResolver.class.getSimpleName());

    public static final ConfigKey<JcloudsPortForwarderExtension> PORT_FORWARDER = ConfigKeys.newConfigKey(
        JcloudsPortForwarderExtension.class, "portforwarding.forwarder", "The port-forwarder to use");

    @SuppressWarnings("serial")
    public static final ConfigKey<Function<Iterable<? extends Image>,Image>> IMAGE_CHOOSER = ConfigKeys.newConfigKey(
        new TypeToken<Function<Iterable<? extends Image>,Image>>() {},
        "imageChooser", "An image chooser function to control which images are preferred", 
        new BrooklynImageChooser().chooser());

    public static final ConfigKey<OsFamily> OS_FAMILY = ConfigKeys.newConfigKey(OsFamily.class, "osFamily", 
        "OS family, e.g. CentOS, Debian, RHEL, Ubuntu");
    public static final ConfigKey<String> OS_VERSION_REGEX = ConfigKeys.newStringConfigKey("osVersionRegex", 
        "Regular expression for the OS version to load");

    public static final ConfigKey<OsFamily> OS_FAMILY_OVERRIDE = ConfigKeys.newConfigKey(OsFamily.class, "osFamilyOverride", 
            "OS family of VMs (ignores VM metadata from jclouds, and assumes this value)");

    public static final ConfigKey<ComputeServiceRegistry> COMPUTE_SERVICE_REGISTRY = ConfigKeys.newConfigKey(
            ComputeServiceRegistry.class,
            "jclouds.computeServiceRegistry",
            "Registry/Factory for creating jclouds ComputeService; default is almost always fine, except where tests want to customize behaviour",
            ComputeServiceRegistryImpl.INSTANCE);

    ConfigKey<Iterable<? extends Module>> COMPUTE_SERVICE_MODULES = ConfigKeys.newConfigKey(
            new TypeToken<Iterable<? extends Module>>() {},
            "jclouds.computeServiceModules", "Optional Guice modules for a jclouds Compute Service Context");

    ConfigKey<JcloudsContext> LINK_CONTEXT = ConfigKeys.newConfigKey(
            JcloudsContext.class,
            "jclouds.linkContext", "Optional link context for jclouds Compute Service Context");
}
