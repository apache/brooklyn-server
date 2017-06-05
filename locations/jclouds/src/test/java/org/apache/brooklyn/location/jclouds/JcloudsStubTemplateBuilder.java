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

import static org.jclouds.ec2.compute.domain.EC2HardwareBuilder.t2_micro;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Provider;

import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.HardwareBuilder;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.ImageBuilder;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.domain.internal.TemplateBuilderImpl;
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
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.domain.RootDeviceType;
import org.jclouds.ec2.domain.VirtualizationType;
import org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions;
import org.jclouds.rest.AuthorizationException;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;

import com.google.common.base.Functions;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class JcloudsStubTemplateBuilder {
    protected final Location provider;
    protected final Location jcloudsDomainLocation;

    protected final String providerName;
    protected final String regionName;
    
    public static TemplateBuilder create() {
        return new JcloudsStubTemplateBuilder().createTemplateBuilder();
    }

    public static TemplateBuilder create(String providerName, String regionName) {
        return new JcloudsStubTemplateBuilder(providerName, regionName).createTemplateBuilder();
    }

    public JcloudsStubTemplateBuilder() {
        this("aws-ec2", "us-east-1");
    }

    public JcloudsStubTemplateBuilder(String providerName, String regionName) {
        this.providerName = providerName;
        this.regionName = regionName;
        this.provider = new LocationBuilder().scope(LocationScope.PROVIDER).id(providerName).description(providerName).build();
        this.jcloudsDomainLocation = new LocationBuilder().scope(LocationScope.REGION).id(this.regionName).description(this.regionName)
                .parent(provider).build();
    }

    public TemplateBuilder createTemplateBuilder() {
        final Supplier<Set<? extends Image>> images = Suppliers.<Set<? extends Image>> ofInstance(ImmutableSet.of(getImage()));
        ImmutableMap<RegionAndName, Image> imageMap = (ImmutableMap<RegionAndName, Image>) ImagesToRegionAndIdMap.imagesToMap(images.get());
        Supplier<LoadingCache<RegionAndName, ? extends Image>> imageCache = Suppliers.<LoadingCache<RegionAndName, ? extends Image>> ofInstance(
                CacheBuilder.newBuilder().<RegionAndName, Image>build(CacheLoader.from(Functions.forMap(imageMap))));
        return newTemplateBuilder(images, imageCache);
    }

    /**
     * Used source from jclouds project.
     * {@link org.jclouds.ec2.compute.EC2TemplateBuilderTest#newTemplateBuilder}
     */
    @SuppressWarnings("unchecked")
    protected TemplateBuilder newTemplateBuilder(Supplier<Set<? extends Image>> images, Supplier<LoadingCache<RegionAndName, ? extends Image>> imageCache) {
        Provider<TemplateOptions> optionsProvider = mock(Provider.class);
        Provider<TemplateBuilder> templateBuilderProvider = mock(Provider.class);
        TemplateOptions defaultOptions = newTemplateOptions();
        final GetImageStrategy getImageStrategy = mock(GetImageStrategy.class);

        when(optionsProvider.get()).thenReturn(defaultOptions);

        Supplier<Set<? extends Location>> locations = Suppliers.<Set<? extends Location>> ofInstance(ImmutableSet
                .of(jcloudsDomainLocation));
        Supplier<Set<? extends Hardware>> sizes = Suppliers.<Set<? extends Hardware>> ofInstance(ImmutableSet
                .of(getHardware()));


//        AtomicReference<AuthorizationException> authException = new AtomicReference<AuthorizationException>(new AuthorizationException());
        AtomicReference<AuthorizationException> authException = new AtomicReference<AuthorizationException>();

        com.google.inject.Provider<GetImageStrategy> imageLoader = new com.google.inject.Provider<GetImageStrategy>() {
            @Override
            public GetImageStrategy get() {
                return getImageStrategy;
            }
        };

        switch (providerName) {
        case "aws-ec2" :
        case "ec2" :
            return new EC2TemplateBuilderImpl(
                    locations,
                    new ImageCacheSupplier(images, 60, authException, imageLoader),
                    sizes,
                    Suppliers.ofInstance(jcloudsDomainLocation),
                    optionsProvider,
                    templateBuilderProvider,
                    imageCache) {
                @Override
                protected ToStringHelper string() {
                    return super.string().add("type", "Stubbed-TemplateBuilder");
                }
            };
        default:
            return new TemplateBuilderImpl(
                    locations,
                    new ImageCacheSupplier(images, 60, authException, imageLoader),
                    sizes,
                    Suppliers.ofInstance(jcloudsDomainLocation),
                    optionsProvider,
                    templateBuilderProvider) {
                @Override
                protected ToStringHelper string() {
                    return super.string().add("type", "Stubbed-TemplateBuilder");
                }
            };
        }
    }
    
    protected TemplateOptions newTemplateOptions() {
        switch (providerName) {
        case "aws-ec2" :
            return new AWSEC2TemplateOptions();
        case "ec2" :
            return new EC2TemplateOptions();
        case "google-compute-engine" :
            return new GoogleComputeEngineTemplateOptions();
            //return mock(GoogleComputeEngineTemplateOptions.class);
        case "azurecompute" :
            return new org.jclouds.azurecompute.compute.options.AzureComputeTemplateOptions();
        case "azurecompute-arm" :
            return new org.jclouds.azurecompute.arm.compute.options.AzureTemplateOptions();
        case "softlayer" :
            return new SoftLayerTemplateOptions();
        default:
            throw new UnsupportedOperationException("Unsupported stubbed TemplateOptions for provider "+providerName);
        }
    }
    
    protected Image getImage() {
        switch (providerName) {
        case "aws-ec2" :
        case "ec2" :
            return new ImageBuilder().providerId("ebs-image-provider").name("image")
                    .id(regionName+"/bogus-image").location(jcloudsDomainLocation)
                    .userMetadata(ImmutableMap.of("rootDeviceType", RootDeviceType.EBS.value()))
                    .operatingSystem(new OperatingSystem(OsFamily.UBUNTU, null, "1.0", VirtualizationType.PARAVIRTUAL.value(), "ubuntu", true))
                    .description("description").version("1.0").defaultCredentials(LoginCredentials.builder().user("root").build())
                    .status(Image.Status.AVAILABLE)
                    .build();
        case "google-compute-engine" :
            return new ImageBuilder().providerId("gce-image-provider").name("image")
                    .id(regionName+"/bogus-image").location(jcloudsDomainLocation)
                    .operatingSystem(new OperatingSystem(OsFamily.UBUNTU, null, "1.0", VirtualizationType.PARAVIRTUAL.value(), "ubuntu", true))
                    .description("description").version("1.0").defaultCredentials(LoginCredentials.builder().user("root").build())
                    .status(Image.Status.AVAILABLE)
                    .build();
        default:
            throw new UnsupportedOperationException("Unsupported stubbed Image for provider "+providerName);
        }
    }
    
    protected Hardware getHardware() {
        switch (providerName) {
        case "aws-ec2" :
        case "ec2" :
            return t2_micro().id("supporting-bogus")
                    .supportsImageIds(ImmutableSet.of("us-east-1/bogus-image"))
                    .virtualizationType(VirtualizationType.PARAVIRTUAL)
                    .rootDeviceType(RootDeviceType.EBS)
                    .build();
        case "google-compute-engine" :
            return new HardwareBuilder()
                    .providerId(providerName)
                    .id("n1-standard-1")
                    .ram(3750)
                    .processor(new Processor(1, 1234))
                    .supportsImage(Predicates.alwaysTrue())
                    .build();
        default:
            throw new UnsupportedOperationException("Unsupported stubbed Hardware for provider "+providerName);
        }
    }
}
