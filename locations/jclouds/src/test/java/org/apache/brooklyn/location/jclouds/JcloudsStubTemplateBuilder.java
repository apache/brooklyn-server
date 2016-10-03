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

import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.jclouds.compute.domain.*;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.compute.strategy.GetImageStrategy;
import org.jclouds.compute.suppliers.ImageCacheSupplier;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.ec2.compute.domain.RegionAndName;
import org.jclouds.ec2.compute.functions.ImagesToRegionAndIdMap;
import org.jclouds.ec2.compute.internal.EC2TemplateBuilderImpl;
import org.jclouds.ec2.domain.RootDeviceType;
import org.jclouds.ec2.domain.VirtualizationType;

import javax.inject.Provider;
import java.util.Set;

import static org.jclouds.ec2.compute.domain.EC2HardwareBuilder.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JcloudsStubTemplateBuilder {
    protected final Location provider = new LocationBuilder().scope(LocationScope.PROVIDER).id("aws-ec2").description("aws-ec2").build();
    protected final Location jcloudsDomainLocation = new LocationBuilder().scope(LocationScope.REGION).id("us-east-1").description("us-east-1")
            .parent(provider).build();

    protected final Hardware HARDWARE_SUPPORTING_BOGUS = t2_micro().id("supporting-bogus")
            .supportsImageIds(ImmutableSet.of("us-east-1/bogus-image"))
            .virtualizationType(VirtualizationType.PARAVIRTUAL)
            .rootDeviceType(RootDeviceType.EBS)
            .build();

    public static TemplateBuilder create() {
        return new JcloudsStubTemplateBuilder().createTemplateBuilder();
    }

    public TemplateBuilder createTemplateBuilder() {
        final Supplier<Set<? extends Image>> images = Suppliers.<Set<? extends Image>> ofInstance(ImmutableSet.of(
                new ImageBuilder().providerId("ebs-image-provider").name("image")
                        .id("us-east-1/bogus-image").location(jcloudsDomainLocation)
                        .userMetadata(ImmutableMap.of("rootDeviceType", RootDeviceType.EBS.value()))
                        .operatingSystem(new OperatingSystem(OsFamily.UBUNTU, null, "1.0", VirtualizationType.PARAVIRTUAL.value(), "ubuntu", true))
                        .description("description").version("1.0").defaultCredentials(LoginCredentials.builder().user("root").build())
                        .status(Image.Status.AVAILABLE)
                        .build()));
        ImmutableMap<RegionAndName, Image> imageMap = (ImmutableMap<RegionAndName, Image>) ImagesToRegionAndIdMap.imagesToMap(images.get());
        Supplier<LoadingCache<RegionAndName, ? extends Image>> imageCache = Suppliers.<LoadingCache<RegionAndName, ? extends Image>> ofInstance(
                CacheBuilder.newBuilder().<RegionAndName, Image>build(CacheLoader.from(Functions.forMap(imageMap))));
        JcloudsStubTemplateBuilder jcloudsStubTemplateBuilder = new JcloudsStubTemplateBuilder();
        return jcloudsStubTemplateBuilder.newTemplateBuilder(images, imageCache);
    }

    /**
     * Used source from jclouds project.
     * {@link org.jclouds.ec2.compute.EC2TemplateBuilderTest#newTemplateBuilder}
     */
    @SuppressWarnings("unchecked")
    protected TemplateBuilder newTemplateBuilder(Supplier<Set<? extends Image>> images, Supplier<LoadingCache<RegionAndName, ? extends Image>> imageCache) {

        Provider<TemplateOptions> optionsProvider = mock(Provider.class);
        Provider<TemplateBuilder> templateBuilderProvider = mock(Provider.class);
        TemplateOptions defaultOptions = mock(TemplateOptions.class);
        GetImageStrategy getImageStrategy = mock(GetImageStrategy.class);

        when(optionsProvider.get()).thenReturn(defaultOptions);

        Supplier<Set<? extends Location>> locations = Suppliers.<Set<? extends Location>> ofInstance(ImmutableSet
                .of(jcloudsDomainLocation));
        Supplier<Set<? extends Hardware>> sizes = Suppliers.<Set<? extends Hardware>> ofInstance(ImmutableSet
                .of(HARDWARE_SUPPORTING_BOGUS));

        return new EC2TemplateBuilderImpl(locations, new ImageCacheSupplier(images, 60), sizes, Suppliers.ofInstance(jcloudsDomainLocation), optionsProvider,
                templateBuilderProvider, getImageStrategy, imageCache) {
        };
    }
}
