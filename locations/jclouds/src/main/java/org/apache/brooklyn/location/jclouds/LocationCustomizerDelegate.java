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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.location.MachineLocationCustomizer;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

public class LocationCustomizerDelegate implements JcloudsLocationCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(LocationCustomizerDelegate.class);

    private Collection<JcloudsLocationCustomizer> customizers;
    private Collection<MachineLocationCustomizer> machineCustomizers;

    public static JcloudsLocationCustomizer newInstance(ManagementContext managementContext, ConfigBag setup) {
        return new LocationCustomizerDelegate(managementContext, setup);
    }

    /** @deprecated since 0.11.0. Use {@link #newInstance(ManagementContext, ConfigBag)} instead. */
    @Deprecated
    public static JcloudsLocationCustomizer newInstance(Collection<JcloudsLocationCustomizer> customizers) {
        return new LocationCustomizerDelegate(customizers);
    }

    private LocationCustomizerDelegate(ManagementContext mgmt, ConfigBag setup) {
        this.customizers = getCustomizers(mgmt, setup);
        this.machineCustomizers = getMachineCustomizers(mgmt, setup);
    }

    private LocationCustomizerDelegate(Collection<JcloudsLocationCustomizer> customizers) {
        this.customizers = customizers;
        this.machineCustomizers = ImmutableList.of();
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateBuilder templateBuilder) {
        // Then apply any optional app-specific customization.
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(location, computeService, templateBuilder);
        }
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, Template template) {
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(location, computeService, template);
        }
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(location, computeService, templateOptions);
        }
    }

    @Override
    public void customize(JcloudsLocation location, NodeMetadata node, ConfigBag setup) {
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(location, node, setup);
        }
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machineLocation) {
        // Apply any optional app-specific customization.
        for (JcloudsLocationCustomizer customizer : customizers) {
            LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
            customizer.customize(location, computeService, machineLocation);
        }
        for (MachineLocationCustomizer customizer : machineCustomizers) {
            LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
            customizer.customize(machineLocation);
        }
    }

    @Override
    public void preRelease(JcloudsMachineLocation machine) {
        Exception tothrow = null;
        for (JcloudsLocationCustomizer customizer : customizers) {
            try {
                customizer.preRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release customizer "+customizer+" for machine "+machine+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        for (MachineLocationCustomizer customizer : machineCustomizers) {
            try {
                customizer.preRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release machine customizer "+customizer+" for machine "+machine+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        if (tothrow != null) {
            throw Exceptions.propagate(tothrow);
        }
    }

    @Override
    public void postRelease(JcloudsMachineLocation machine) {
        Exception tothrow = null;
        for (JcloudsLocationCustomizer customizer : customizers) {
            try {
                customizer.postRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release customizer "+customizer+" for machine "+machine+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        if (tothrow != null) {
            throw Exceptions.propagate(tothrow);
        }
    }

    @SuppressWarnings("deprecation")
    public static Collection<JcloudsLocationCustomizer> getCustomizers(ManagementContext mgmt, ConfigBag setup) {
        JcloudsLocationCustomizer customizer = setup.get(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZER);
        Collection<JcloudsLocationCustomizer> customizers = setup.get(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS);
        String customizerType = setup.get(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZER_TYPE);
        String customizersSupplierType = setup.get(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS_SUPPLIER_TYPE);

        ClassLoader catalogClassLoader = mgmt.getCatalogClassLoader();
        List<JcloudsLocationCustomizer> result = new ArrayList<JcloudsLocationCustomizer>();
        if (customizer != null) result.add(customizer);
        if (customizers != null) result.addAll(customizers);
        if (Strings.isNonBlank(customizerType)) {
            Maybe<JcloudsLocationCustomizer> customizerByType = Reflections.invokeConstructorFromArgs(catalogClassLoader, JcloudsLocationCustomizer.class, customizerType, setup);
            if (customizerByType.isPresent()) {
                result.add(customizerByType.get());
            } else {
                customizerByType = Reflections.invokeConstructorFromArgs(catalogClassLoader, JcloudsLocationCustomizer.class, customizerType);
                if (customizerByType.isPresent()) {
                    result.add(customizerByType.get());
                } else {
                    throw new IllegalStateException("Failed to create JcloudsLocationCustomizer "+customizersSupplierType);
                }
            }
        }
        if (Strings.isNonBlank(customizersSupplierType)) {
            Maybe<Supplier<Collection<JcloudsLocationCustomizer>>> supplier = Reflections.<Supplier<Collection<JcloudsLocationCustomizer>>>invokeConstructorFromArgsUntyped(catalogClassLoader, customizersSupplierType, setup);
            if (supplier.isPresent()) {
                result.addAll(supplier.get().get());
            } else {
                supplier = Reflections.<Supplier<Collection<JcloudsLocationCustomizer>>>invokeConstructorFromArgsUntyped(catalogClassLoader, customizersSupplierType);
                if (supplier.isPresent()) {
                    result.addAll(supplier.get().get());
                } else {
                    throw new IllegalStateException("Failed to create JcloudsLocationCustomizer supplier "+customizersSupplierType);
                }
            }
        }
        return result;
    }

    protected static Collection<MachineLocationCustomizer> getMachineCustomizers(ManagementContext mgmt, ConfigBag setup) {
        Collection<MachineLocationCustomizer> customizers = setup.get(JcloudsLocationConfig.MACHINE_LOCATION_CUSTOMIZERS);
        return (customizers == null ? ImmutableList.<MachineLocationCustomizer>of() : customizers);
    }

}
