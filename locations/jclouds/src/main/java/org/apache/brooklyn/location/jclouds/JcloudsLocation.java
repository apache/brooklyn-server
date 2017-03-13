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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;
import static org.apache.brooklyn.util.ssh.BashCommands.sbinPath;
import static org.jclouds.util.Throwables2.getFirstThrowableOfType;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.xml.ws.WebServiceException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineLocationCustomizer;
import org.apache.brooklyn.api.location.MachineManagementMixins;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.AccessController;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.BasicMachineMetadata;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.location.LocationConfigUtils.OsCredential;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.location.access.PortMapping;
import org.apache.brooklyn.core.location.cloud.AbstractCloudMachineProvisioningLocation;
import org.apache.brooklyn.core.location.cloud.AvailabilityZoneExtension;
import org.apache.brooklyn.core.location.cloud.names.AbstractCloudMachineNamer;
import org.apache.brooklyn.core.location.cloud.names.CloudMachineNamer;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.internal.LocalLocationManager;
import org.apache.brooklyn.core.mgmt.persist.LocationWithObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.persist.jclouds.JcloudsBlobStoreBasedObjectStore;
import org.apache.brooklyn.location.jclouds.networking.JcloudsPortForwarderExtension;
import org.apache.brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import org.apache.brooklyn.location.jclouds.templates.customize.TemplateBuilderCustomizer;
import org.apache.brooklyn.location.jclouds.templates.customize.TemplateBuilderCustomizers;
import org.apache.brooklyn.location.jclouds.templates.customize.TemplateOptionCustomizer;
import org.apache.brooklyn.location.jclouds.templates.customize.TemplateOptionCustomizers;
import org.apache.brooklyn.location.jclouds.zone.AwsAvailabilityZoneExtension;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.internal.ssh.ShellTool;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmToolResponse;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.DynamicTasks.TaskQueueingResult;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.TaskInternal;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.net.Cidr;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.Protocol;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.ssh.IptablesCommands;
import org.apache.brooklyn.util.ssh.IptablesCommands.Chain;
import org.apache.brooklyn.util.ssh.IptablesCommands.Policy;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.KeyValueParser;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.config.AdminAccessConfiguration;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.rest.AuthorizationException;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import org.jclouds.scriptbuilder.functions.InitAdminAccess;
import org.jclouds.scriptbuilder.statements.ssh.AuthorizeRSAPublicKeys;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.jclouds.util.Predicates2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.net.HostAndPort;

/**
 * For provisioning and managing VMs in a particular provider/region, using jclouds.
 * Configuration flags are defined in {@link JcloudsLocationConfig}.
 */
public class JcloudsLocation extends AbstractCloudMachineProvisioningLocation implements
        JcloudsLocationConfig, MachineManagementMixins.RichMachineProvisioningLocation<MachineLocation>,
        LocationWithObjectStore, MachineManagementMixins.SuspendResumeLocation {

    // TODO After converting from Groovy to Java, this is now very bad code! It relies entirely on putting
    // things into and taking them out of maps; it's not type-safe, and it's thus very error-prone.
    // In Groovy, that's considered ok but not in Java.

    // TODO test (and fix) ability to set config keys from flags

    // TODO we say config is inherited, but it isn't the case for many "deep" / jclouds properties
    // e.g. when we pass getRawLocalConfigBag() in and decorate it with additional flags
    // (inheritance only works when we call getConfig in this class)

    public static final Logger LOG = LoggerFactory.getLogger(JcloudsLocation.class);

    public static final String ROOT_USERNAME = "root";
    /** these userNames are known to be the preferred/required logins in some common/default images
     *  where root@ is not allowed to log in */
    public static final List<String> ROOT_ALIASES = ImmutableList.of("ubuntu", "centos", "ec2-user");
    public static final List<String> COMMON_USER_NAMES_TO_TRY = ImmutableList.<String>builder().add(ROOT_USERNAME).addAll(ROOT_ALIASES).add("admin").build();

    private static final int NOTES_MAX_LENGTH = 1000;

    @VisibleForTesting
    static final String AWS_VPC_HELP_URL = "http://brooklyn.apache.org/v/latest/ops/locations/index.html#ec2-classic-problems-with-vpc-only-hardware-instance-types";

    private final AtomicBoolean listedAvailableTemplatesOnNoSuchTemplate = new AtomicBoolean(false);

    private final Map<String,Map<String, ? extends Object>> tagMapping = Maps.newLinkedHashMap();

    @SetFromFlag // so it's persisted
    private final Map<MachineLocation,String> vmInstanceIds = Maps.newLinkedHashMap();

    static { Networking.init(); }

    public JcloudsLocation() {
        super();
    }

    /** typically wants at least ACCESS_IDENTITY and ACCESS_CREDENTIAL */
    public JcloudsLocation(Map<?,?> conf) {
       super(conf);
    }

    @Override
    @Deprecated
    public JcloudsLocation configure(Map<?,?> properties) {
        super.configure(properties);

        if (config().getLocalBag().containsKey("providerLocationId")) {
            LOG.warn("Using deprecated 'providerLocationId' key in "+this);
            if (!config().getLocalBag().containsKey(CLOUD_REGION_ID))
                config().putAll(MutableMap.of(CLOUD_REGION_ID.getName(), (String)config().getLocalBag().getStringKey("providerLocationId")));
        }

        if (isDisplayNameAutoGenerated() || !groovyTruth(getDisplayName())) {
            setDisplayName(elvis(getProvider(), "unknown") +
                   (groovyTruth(getRegion()) ? ":"+getRegion() : "") +
                   (groovyTruth(getEndpoint()) ? ":"+getEndpoint() : ""));
        }

        if (getConfig(MACHINE_CREATION_SEMAPHORE) == null) {
            Integer maxConcurrent = getConfig(MAX_CONCURRENT_MACHINE_CREATIONS);
            if (maxConcurrent == null || maxConcurrent < 1) {
                throw new IllegalStateException(MAX_CONCURRENT_MACHINE_CREATIONS.getName() + " must be >= 1, but was "+maxConcurrent);
            }
            config().set(MACHINE_CREATION_SEMAPHORE, new Semaphore(maxConcurrent, true));
        }
        return this;
    }

    @Override
    public void init() {
        super.init();
        if ("aws-ec2".equals(getProvider())) {
            addExtension(AvailabilityZoneExtension.class, new AwsAvailabilityZoneExtension(getManagementContext(), this));
        }
    }

    @Override
    public JcloudsLocation newSubLocation(Map<?,?> newFlags) {
        return newSubLocation(getClass(), newFlags);
    }

    @Override
    public JcloudsLocation newSubLocation(Class<? extends AbstractCloudMachineProvisioningLocation> type, Map<?,?> newFlags) {
        // TODO should be able to use ConfigBag.newInstanceExtending; would require moving stuff around to api etc
        return (JcloudsLocation) getManagementContext().getLocationManager().createLocation(LocationSpec.create(type)
                .parent(this)
                .configure(config().getLocalBag().getAllConfig())  // FIXME Should this just be inherited?
                .configure(MACHINE_CREATION_SEMAPHORE, getMachineCreationSemaphore())
                .configure(newFlags));
    }

    @Override
    public String toString() {
        Object identity = getIdentity();
        String configDescription = config().getLocalBag().getDescription();
        if (configDescription!=null && configDescription.startsWith(getClass().getSimpleName()))
            return configDescription;
        return getClass().getSimpleName()+"["+getDisplayName()+":"+(identity != null ? identity : null)+
                (configDescription!=null ? "/"+configDescription : "") + "@" + getId() + "]";
    }

    @Override
    public String toVerboseString() {
        return Objects.toStringHelper(this).omitNullValues()
                .add("id", getId()).add("name", getDisplayName()).add("identity", getIdentity())
                .add("description", config().getLocalBag().getDescription()).add("provider", getProvider())
                .add("region", getRegion()).add("endpoint", getEndpoint())
                .toString();
    }

    public String getProvider() {
        return getConfig(CLOUD_PROVIDER);
    }

    public String getIdentity() {
        return getConfig(ACCESS_IDENTITY);
    }

    public String getCredential() {
        return getConfig(ACCESS_CREDENTIAL);
    }

    /** returns the location ID used by the provider, if set, e.g. us-west-1 */
    public String getRegion() {
        return getConfig(CLOUD_REGION_ID);
    }

    public String getEndpoint() {
        return (String) config().getBag().getWithDeprecation(CLOUD_ENDPOINT, JCLOUDS_KEY_ENDPOINT);
    }

    public String getUser(ConfigBag config) {
        return (String) config.getWithDeprecation(USER, JCLOUDS_KEY_USERNAME);
    }

    public boolean isWindows(Template template, ConfigBag config) {
        return isWindows(template.getImage(), config);
    }

    /**
     * Whether VMs provisioned from this image will be Windows. Assume windows if the image
     * explicitly says so, or if image does not tell us then fall back to whether the config
     * explicitly says windows in {@link JcloudsLocationConfig#OS_FAMILY}.
     *
     * Will first look at {@link JcloudsLocationConfig#OS_FAMILY_OVERRIDE}, to check if that
     * is set. If so, no further checks are done: the value is compared against {@link OsFamily#WINDOWS}.
     *
     * We believe the config (e.g. from brooklyn.properties) because for some clouds there is
     * insufficient meta-data so the Image might not tell us. Thus a user can work around it
     * by explicitly supplying configuration.
     */
    public boolean isWindows(Image image, ConfigBag config) {
        OsFamily override = config.get(OS_FAMILY_OVERRIDE);
        if (override != null) return override == OsFamily.WINDOWS;

        OsFamily confFamily = config.get(OS_FAMILY);
        OperatingSystem os = (image != null) ? image.getOperatingSystem() : null;
        return (os != null && os.getFamily() != OsFamily.UNRECOGNIZED)
                ? (OsFamily.WINDOWS == os.getFamily())
                : (OsFamily.WINDOWS == confFamily);
    }

    /**
     * Whether the given VM is Windows.
     *
     * @see #isWindows(Image, ConfigBag)
     */
    public boolean isWindows(NodeMetadata node, ConfigBag config) {
        OsFamily override = config.get(OS_FAMILY_OVERRIDE);
        if (override != null) return override == OsFamily.WINDOWS;

        OsFamily confFamily = config.get(OS_FAMILY);
        OperatingSystem os = (node != null) ? node.getOperatingSystem() : null;
        return (os != null && os.getFamily() != OsFamily.UNRECOGNIZED)
                ? (OsFamily.WINDOWS == os.getFamily())
                : (OsFamily.WINDOWS == confFamily);
    }

    public boolean isLocationFirewalldEnabled(SshMachineLocation location) {
        int result = location.execCommands("checking if firewalld is active",
                ImmutableList.of(IptablesCommands.firewalldServiceIsActive()));
        return result == 0;
    }

    protected Semaphore getMachineCreationSemaphore() {
        return checkNotNull(getConfig(MACHINE_CREATION_SEMAPHORE), MACHINE_CREATION_SEMAPHORE.getName());
    }

    protected CloudMachineNamer getCloudMachineNamer(ConfigBag config) {
        String namerClass = config.get(LocationConfigKeys.CLOUD_MACHINE_NAMER_CLASS);
        if (Strings.isNonBlank(namerClass)) {
            Maybe<CloudMachineNamer> cloudNamer = Reflections.invokeConstructorFromArgs(getManagementContext().getCatalogClassLoader(), CloudMachineNamer.class, namerClass);
            if (cloudNamer.isPresent()) {
                return cloudNamer.get();
            } else {
                throw new IllegalStateException("Failed to create CloudMachineNamer "+namerClass+" for location "+this);
            }
        } else {
            return new JcloudsMachineNamer();
        }
    }

    public Collection<JcloudsLocationCustomizer> getCustomizers(ConfigBag setup) {
        @SuppressWarnings("deprecation")
        JcloudsLocationCustomizer customizer = setup.get(JCLOUDS_LOCATION_CUSTOMIZER);
        Collection<JcloudsLocationCustomizer> customizers = setup.get(JCLOUDS_LOCATION_CUSTOMIZERS);
        @SuppressWarnings("deprecation")
        String customizerType = setup.get(JCLOUDS_LOCATION_CUSTOMIZER_TYPE);
        @SuppressWarnings("deprecation")
        String customizersSupplierType = setup.get(JCLOUDS_LOCATION_CUSTOMIZERS_SUPPLIER_TYPE);

        ClassLoader catalogClassLoader = getManagementContext().getCatalogClassLoader();
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
                    throw new IllegalStateException("Failed to create JcloudsLocationCustomizer "+customizersSupplierType+" for location "+this);
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
                    throw new IllegalStateException("Failed to create JcloudsLocationCustomizer supplier "+customizersSupplierType+" for location "+this);
                }
            }
        }
        return result;
    }

    public ConnectivityResolver getLocationNetworkInfoCustomizer(ConfigBag setup) {
        ConnectivityResolver configured = setup.get(CONNECTIVITY_RESOLVER);
        return (configured != null) ? configured : new DefaultConnectivityResolver();
    }

    protected Collection<MachineLocationCustomizer> getMachineCustomizers(ConfigBag setup) {
        Collection<MachineLocationCustomizer> customizers = setup.get(MACHINE_LOCATION_CUSTOMIZERS);
        return (customizers == null ? ImmutableList.<MachineLocationCustomizer>of() : customizers);
    }

    public void setDefaultImageId(String val) {
        config().set(DEFAULT_IMAGE_ID, val);
    }

    // TODO remove tagMapping, or promote it
    // (i think i favour removing it, letting the config come in from the entity)

    public void setTagMapping(Map<String,Map<String, ? extends Object>> val) {
        tagMapping.clear();
        tagMapping.putAll(val);
    }

    // TODO Decide on semantics. If I give "TomcatServer" and "Ubuntu", then must I get back an image that matches both?
    // Currently, just takes first match that it finds...
    @Override
    public Map<String,Object> getProvisioningFlags(Collection<String> tags) {
        Map<String,Object> result = Maps.newLinkedHashMap();
        Collection<String> unmatchedTags = Lists.newArrayList();
        for (String it : tags) {
            if (groovyTruth(tagMapping.get(it)) && !groovyTruth(result)) {
                result.putAll(tagMapping.get(it));
            } else {
                unmatchedTags.add(it);
            }
        }
        if (unmatchedTags.size() > 0) {
            LOG.debug("Location {}, failed to match provisioning tags {}", this, unmatchedTags);
        }
        return result;
    }

    public static final Set<ConfigKey<?>> getAllSupportedProperties() {
        Set<String> configsOnClass = Sets.newLinkedHashSet(
            Iterables.transform(ConfigUtils.getStaticKeysOnClass(JcloudsLocation.class),
                new Function<HasConfigKey<?>,String>() {
                    @Override @Nullable
                    public String apply(@Nullable HasConfigKey<?> input) {
                        return input.getConfigKey().getName();
                    }
                }));
        Set<ConfigKey<?>> configKeysInList = ImmutableSet.<ConfigKey<?>>builder()
                .addAll(SUPPORTED_TEMPLATE_BUILDER_PROPERTIES.keySet())
                .addAll(SUPPORTED_TEMPLATE_OPTIONS_PROPERTIES.keySet())
                .build();
        Set<String> configsInList = Sets.newLinkedHashSet(
            Iterables.transform(configKeysInList,
            new Function<ConfigKey<?>,String>() {
                @Override @Nullable
                public String apply(@Nullable ConfigKey<?> input) {
                    return input.getName();
                }
            }));

        SetView<String> extrasInList = Sets.difference(configsInList, configsOnClass);
        // notInList is normal
        if (!extrasInList.isEmpty())
            LOG.warn("JcloudsLocation supported properties differs from config defined on class: " + extrasInList);
        return Collections.unmodifiableSet(configKeysInList);
    }

    public ComputeService getComputeService() {
        return getComputeService(MutableMap.of());
    }
    public ComputeService getComputeService(Map<?,?> flags) {
        ConfigBag conf = (flags==null || flags.isEmpty())
                ? config().getBag()
                : ConfigBag.newInstanceExtending(config().getBag(), flags);
        return getComputeService(conf);
    }

    public ComputeService getComputeService(ConfigBag config) {
        ComputeServiceRegistry registry = getConfig(COMPUTE_SERVICE_REGISTRY);
        return registry.findComputeService(ResolvingConfigBag.newInstanceExtending(getManagementContext(), config), true);
    }

    /** @deprecated since 0.7.0 use {@link #listMachines()} */ @Deprecated
    public Set<? extends ComputeMetadata> listNodes() {
        return listNodes(MutableMap.of());
    }
    /** @deprecated since 0.7.0 use {@link #listMachines()}.
     * (no support for custom compute service flags; if that is needed, we'll have to introduce a new method,
     * but it seems there are no usages) */ @Deprecated
    public Set<? extends ComputeMetadata> listNodes(Map<?,?> flags) {
        return getComputeService(flags).listNodes();
    }

    @Override
    public Map<String, MachineManagementMixins.MachineMetadata> listMachines() {
        Set<? extends ComputeMetadata> nodes =
            getRegion()!=null ? getComputeService().listNodesDetailsMatching(JcloudsPredicates.nodeInLocation(getRegion(), true))
                : getComputeService().listNodes();
        Map<String,MachineManagementMixins.MachineMetadata> result = new LinkedHashMap<String, MachineManagementMixins.MachineMetadata>();

        for (ComputeMetadata node: nodes)
            result.put(node.getId(), getMachineMetadata(node));

        return result;
    }

    protected MachineManagementMixins.MachineMetadata getMachineMetadata(ComputeMetadata node) {
        if (node==null)
            return null;
        return new BasicMachineMetadata(node.getId(), node.getName(),
            ((node instanceof NodeMetadata) ? Iterators.tryFind( ((NodeMetadata)node).getPublicAddresses().iterator(), Predicates.alwaysTrue() ).orNull() : null),
            ((node instanceof NodeMetadata) ? ((NodeMetadata)node).getStatus()==Status.RUNNING : null),
            node);
    }

    @Override
    public MachineManagementMixins.MachineMetadata getMachineMetadata(MachineLocation l) {
        if (l instanceof JcloudsSshMachineLocation) {
            return getMachineMetadata(getComputeService().getNodeMetadata(((JcloudsSshMachineLocation) l).getJcloudsId()));
        }
        return null;
    }

    @Override
    public void killMachine(String cloudServiceId) {
        getComputeService().destroyNode(cloudServiceId);
    }

    @Override
    public void killMachine(MachineLocation l) {
        MachineManagementMixins.MachineMetadata m = getMachineMetadata(l);
        if (m==null) throw new NoSuchElementException("Machine "+l+" is not known at "+this);
        killMachine(m.getId());
    }

    /** can generate a string describing where something is being created
     * (provider, region/location and/or endpoint, callerContext);
     * previously set on the config bag, but not any longer (Sept 2016) as config is treated like entities */
    protected String getCreationString(ConfigBag config) {
        return elvis(config.get(CLOUD_PROVIDER), "unknown")+
                (config.containsKey(CLOUD_REGION_ID) ? ":"+config.get(CLOUD_REGION_ID) : "")+
                (config.containsKey(CLOUD_ENDPOINT) ? ":"+config.get(CLOUD_ENDPOINT) : "")+
                (config.containsKey(CALLER_CONTEXT) ? "@"+config.get(CALLER_CONTEXT) : "");
    }

    // ----------------- obtaining a new machine ------------------------
    public MachineLocation obtain() throws NoMachinesAvailableException {
        return obtain(MutableMap.of());
    }
    public MachineLocation obtain(TemplateBuilder tb) throws NoMachinesAvailableException {
        return obtain(MutableMap.of(), tb);
    }
    public MachineLocation obtain(Map<?,?> flags, TemplateBuilder tb) throws NoMachinesAvailableException {
        return obtain(MutableMap.builder().putAll(flags).put(TEMPLATE_BUILDER, tb).build());
    }

    /** core method for obtaining a VM using jclouds;
     * Map should contain CLOUD_PROVIDER and CLOUD_ENDPOINT or CLOUD_REGION, depending on the cloud,
     * as well as ACCESS_IDENTITY and ACCESS_CREDENTIAL,
     * plus any further properties to specify e.g. images, hardware profiles, accessing user
     * (for initial login, and a user potentially to create for subsequent ie normal access) */
    @Override
    public MachineLocation obtain(Map<?,?> flags) throws NoMachinesAvailableException {
        ConfigBag setupRaw = ConfigBag.newInstanceExtending(config().getBag(), flags);
        ConfigBag setup = ResolvingConfigBag.newInstanceExtending(getManagementContext(), setupRaw);

        Map<String, Object> flagTemplateOptions = ConfigBag.newInstance(flags).get(TEMPLATE_OPTIONS);
        Map<String, Object> baseTemplateOptions = config().get(TEMPLATE_OPTIONS);
        Map<String, Object> templateOptions = (Map<String, Object>) shallowMerge(Maybe.fromNullable(flagTemplateOptions), Maybe.fromNullable(baseTemplateOptions), TEMPLATE_OPTIONS).orNull();
        setup.put(TEMPLATE_OPTIONS, templateOptions);

        Integer attempts = setup.get(MACHINE_CREATE_ATTEMPTS);
        List<Exception> exceptions = Lists.newArrayList();
        if (attempts == null || attempts < 1) attempts = 1;
        for (int i = 1; i <= attempts; i++) {
            try {
                return obtainOnce(setup);
            } catch (RuntimeException e) {
                LOG.warn("Attempt #{}/{} to obtain machine threw error: {}", new Object[]{i, attempts, e});
                exceptions.add(e);
            }
        }
        String msg = String.format("Failed to get VM after %d attempt%s.", attempts, attempts == 1 ? "" : "s");

        Exception cause = (exceptions.size() == 1)
                ? exceptions.get(0)
                : new CompoundRuntimeException(msg + " - "
                    + "First cause is "+exceptions.get(0)+" (listed in primary trace); "
                    + "plus " + (exceptions.size()-1) + " more (e.g. the last is "+exceptions.get(exceptions.size()-1)+")",
                    exceptions.get(0), exceptions);

        if (exceptions.get(exceptions.size()-1) instanceof NoMachinesAvailableException) {
            throw new NoMachinesAvailableException(msg, cause);
        } else {
            throw Exceptions.propagate(cause);
        }
    }

    protected ConnectivityResolverOptions.Builder getConnectivityOptionsBuilder(ConfigBag setup, boolean isWindows) {
        boolean waitForSshable = !"false".equalsIgnoreCase(setup.get(JcloudsLocationConfig.WAIT_FOR_SSHABLE));
        boolean waitForWinRmable = !"false".equalsIgnoreCase(setup.get(JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE));
        boolean waitForConnectable = isWindows ? waitForWinRmable : waitForSshable;

        boolean usePortForwarding = setup.get(JcloudsLocationConfig.USE_PORT_FORWARDING);
        boolean skipJcloudsSshing = usePortForwarding ||
                Boolean.FALSE.equals(setup.get(JcloudsLocationConfig.USE_JCLOUDS_SSH_INIT));

        ConnectivityResolverOptions.Builder builder = ConnectivityResolverOptions.builder()
                .waitForConnectable(waitForConnectable)
                .usePortForwarding(usePortForwarding)
                .skipJcloudsSshing(skipJcloudsSshing);

        String pollForFirstReachable = setup.get(JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS);
        boolean pollEnabled = !"false".equalsIgnoreCase(pollForFirstReachable);

        if (pollEnabled) {
            Predicate<? super HostAndPort> reachableAddressesPredicate = getReachableAddressesPredicate(setup);
            Duration pollTimeout = "true".equals(pollForFirstReachable)
                                   ? Duration.FIVE_MINUTES
                                   : Duration.of(pollForFirstReachable);
            builder.pollForReachableAddresses(reachableAddressesPredicate, pollTimeout, true);
        }
        return builder;
    }

    protected MachineLocation obtainOnce(ConfigBag setup) throws NoMachinesAvailableException {
        AccessController.Response access = getManagementContext().getAccessController().canProvisionLocation(this);
        if (!access.isAllowed()) {
            throw new IllegalStateException("Access controller forbids provisioning in "+this+": "+access.getMsg());
        }

        Predicate<? super HostAndPort> reachablePredicate = getReachableAddressesPredicate(setup);
        ConnectivityResolverOptions options = getConnectivityOptionsBuilder(setup, false).build();

        // FIXME How do we influence the node.getLoginPort, so it is set correctly for Windows?
        // Setup port-forwarding, if required
        JcloudsPortForwarderExtension portForwarder = setup.get(PORT_FORWARDER);
        if (options.usePortForwarding()) checkNotNull(portForwarder, "portForwarder, when use-port-forwarding enabled");

        final ComputeService computeService = getComputeService(setup);
        CloudMachineNamer cloudMachineNamer = getCloudMachineNamer(setup);
        String groupId = elvis(setup.get(GROUP_ID), cloudMachineNamer.generateNewGroupId(setup));
        NodeMetadata node = null;
        JcloudsMachineLocation machineLocation = null;
        Duration semaphoreTimestamp = null;
        Duration templateTimestamp = null;
        Duration provisionTimestamp = null;
        Duration usableTimestamp = null;
        Duration customizedTimestamp = null;
        Stopwatch provisioningStopwatch = Stopwatch.createStarted();

        try {
            LOG.info("Creating VM "+getCreationString(setup)+" in "+this);

            Semaphore machineCreationSemaphore = getMachineCreationSemaphore();
            boolean acquired = machineCreationSemaphore.tryAcquire(0, TimeUnit.SECONDS);
            if (!acquired) {
                LOG.info("Waiting in {} for machine-creation permit ({} other queuing requests already)", new Object[] {this, machineCreationSemaphore.getQueueLength()});
                Stopwatch blockStopwatch = Stopwatch.createStarted();
                machineCreationSemaphore.acquire();
                LOG.info("Acquired in {} machine-creation permit, after waiting {}", this, Time.makeTimeStringRounded(blockStopwatch));
            } else {
                LOG.debug("Acquired in {} machine-creation permit immediately", this);
            }
            semaphoreTimestamp = Duration.of(provisioningStopwatch);

            LoginCredentials userCredentials = null;
            Set<? extends NodeMetadata> nodes;
            Template template;
            Collection<JcloudsLocationCustomizer> customizers = getCustomizers(setup);
            Collection<MachineLocationCustomizer> machineCustomizers = getMachineCustomizers(setup);

            try {
                // Setup the template
                template = buildTemplate(computeService, setup, customizers);
                boolean expectWindows = isWindows(template, setup);
                if (!options.skipJcloudsSshing()) {
                    if (expectWindows) {
                        // TODO Was this too early to look at template.getImage? e.g. customizeTemplate could subsequently modify it.
                        LOG.warn("Ignoring invalid configuration for Windows provisioning of "+template.getImage()+": "+USE_JCLOUDS_SSH_INIT.getName()+" should be false");
                        options = options.toBuilder()
                                .skipJcloudsSshing(true)
                                .build();
                    } else if (options.waitForConnectable()) {
                        userCredentials = initTemplateForCreateUser(template, setup);
                    }
                }

                templateTimestamp = Duration.of(provisioningStopwatch);
                // "Name" metadata seems to set the display name; at least in AWS
                // TODO it would be nice if this salt comes from the location's ID (but we don't know that yet as the ssh machine location isn't created yet)
                // TODO in softlayer we want to control the suffix of the hostname which is 3 random hex digits
                template.getOptions().getUserMetadata().put("Name", cloudMachineNamer.generateNewMachineUniqueNameFromGroupId(setup, groupId));

                if (setup.get(JcloudsLocationConfig.INCLUDE_BROOKLYN_USER_METADATA)) {
                    template.getOptions().getUserMetadata().put("brooklyn-user", System.getProperty("user.name"));

                    Object context = setup.get(CALLER_CONTEXT);
                    if (context instanceof Entity) {
                        Entity entity = (Entity)context;
                        template.getOptions().getUserMetadata().put("brooklyn-app-id", entity.getApplicationId());
                        template.getOptions().getUserMetadata().put("brooklyn-app-name", entity.getApplication().getDisplayName());
                        template.getOptions().getUserMetadata().put("brooklyn-entity-id", entity.getId());
                        template.getOptions().getUserMetadata().put("brooklyn-entity-name", entity.getDisplayName());
                        template.getOptions().getUserMetadata().put("brooklyn-server-creation-date", Time.makeDateSimpleStampString());
                    }
                }

                customizeTemplate(computeService, template, customizers);

                LOG.debug("jclouds using template {} / options {} to provision machine in {}",
                        new Object[] {template, template.getOptions(), getCreationString(setup)});

                nodes = computeService.createNodesInGroup(groupId, 1, template);
                provisionTimestamp = Duration.of(provisioningStopwatch);
            } finally {
                machineCreationSemaphore.release();
            }

            node = Iterables.getOnlyElement(nodes, null);
            LOG.debug("jclouds created {} for {}", node, getCreationString(setup));
            if (node == null)
                throw new IllegalStateException("No nodes returned by jclouds create-nodes in " + getCreationString(setup));

            boolean windows = isWindows(node, setup);

            if (windows) {
                int newLoginPort = node.getLoginPort() == 22
                        ? (getConfig(WinRmMachineLocation.USE_HTTPS_WINRM) ? 5986 : 5985)
                        : node.getLoginPort();
                String newLoginUser = "root".equals(node.getCredentials().getUser())
                        ? "Administrator"
                        : node.getCredentials().getUser();
                LOG.debug("jclouds created Windows VM {}; transforming connection details: loginPort from {} to {}; loginUser from {} to {}",
                        new Object[] {node, node.getLoginPort(), newLoginPort, node.getCredentials().getUser(), newLoginUser});
                node = NodeMetadataBuilder.fromNodeMetadata(node)
                        .loginPort(newLoginPort)
                        .credentials(LoginCredentials.builder(node.getCredentials()).user(newLoginUser).build())
                        .build();
            }
            Optional<HostAndPort> portForwardSshOverride;
            if (options.usePortForwarding()) {
                portForwardSshOverride = Optional.of(portForwarder.openPortForwarding(
                        node,
                        node.getLoginPort(),
                        Optional.<Integer>absent(),
                        Protocol.TCP,
                        Cidr.UNIVERSAL));
            } else {
                portForwardSshOverride = Optional.absent();
            }

            options = options.toBuilder()
                    .isWindows(windows)
                    .defaultLoginPort(node.getLoginPort())
                    .portForwardSshOverride(portForwardSshOverride.orNull())
                    .initialCredentials(node.getCredentials())
                    .userCredentials(userCredentials)
                    .build();

            ConnectivityResolver networkInfoCustomizer = getLocationNetworkInfoCustomizer(setup);

            ManagementAddressResolveResult hostPortCred = networkInfoCustomizer.resolve(this, node, setup, options);
            final HostAndPort managementHostAndPort = hostPortCred.hostAndPort();
            LoginCredentials creds = hostPortCred.credentials();
            LOG.info("Using host-and-port={} and user={} when connecting to {}",
                    new Object[]{managementHostAndPort, creds.getUser(), node});

            if (options.skipJcloudsSshing() && options.waitForConnectable()) {
                LoginCredentials createdCredentials = createUser(computeService, node, managementHostAndPort, creds, setup);
                if (createdCredentials != null) {
                    userCredentials = createdCredentials;
                }
            }
            if (userCredentials == null) {
                userCredentials = creds;
            }

            // store the credentials, in case they have changed
            putIfPresentButDifferent(setup, JcloudsLocationConfig.PASSWORD, userCredentials.getOptionalPassword().orNull());
            putIfPresentButDifferent(setup, JcloudsLocationConfig.PRIVATE_KEY_DATA, userCredentials.getOptionalPrivateKey().orNull());

            // Wait for the VM to be reachable over SSH
            if (options.waitForConnectable() && !options.isWindows()) {
                waitForSshable(computeService, node, managementHostAndPort, ImmutableList.of(userCredentials), setup);
            } else {
                LOG.debug("Skipping ssh check for {} ({}) due to config waitForConnectable={}, windows={}",
                        new Object[]{node, getCreationString(setup), options.waitForConnectable(), windows});
            }

            // Do not store the credentials on the node as this may leak the credentials if they
            // are obtained from an external supplier
            node = NodeMetadataBuilder.fromNodeMetadata(node).credentials(null).build();

            usableTimestamp = Duration.of(provisioningStopwatch);

            // Create a JcloudsSshMachineLocation, and register it
            if (windows) {
                machineLocation = registerWinRmMachineLocation(computeService, node, Optional.fromNullable(template), userCredentials, managementHostAndPort, setup);
            } else {
                machineLocation = registerJcloudsSshMachineLocation(computeService, node, Optional.fromNullable(template), userCredentials, managementHostAndPort, setup);
            }

            PortForwardManager portForwardManager = setup.get(PORT_FORWARDING_MANAGER);
            if (portForwardManager == null) {
                LOG.debug("No PortForwardManager, using default");
                portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
            }

            if (options.usePortForwarding() && portForwardSshOverride.isPresent()) {
                // Now that we have the sshMachineLocation, we can associate the port-forwarding address with it.
                portForwardManager.associate(node.getId(), portForwardSshOverride.get(), machineLocation, node.getLoginPort());
            }

            if ("docker".equals(this.getProvider())) {
                if (windows) {
                    throw new UnsupportedOperationException("Docker not supported on Windows");
                }
                Map<Integer, Integer> portMappings = JcloudsUtil.dockerPortMappingsFor(this, node.getId());
                for(Integer containerPort : portMappings.keySet()) {
                    Integer hostPort = portMappings.get(containerPort);
                    String dockerHost = ((JcloudsSshMachineLocation)machineLocation).getSshHostAndPort().getHostText();
                    portForwardManager.associate(node.getId(), HostAndPort.fromParts(dockerHost, hostPort), machineLocation, containerPort);
                }
            }

            List<String> customisationForLogging = new ArrayList<String>();
            // Apply same securityGroups rules to iptables, if iptables is running on the node
            if (options.waitForConnectable()) {

                String setupScript = setup.get(JcloudsLocationConfig.CUSTOM_MACHINE_SETUP_SCRIPT_URL);
                List<String> setupScripts = setup.get(JcloudsLocationConfig.CUSTOM_MACHINE_SETUP_SCRIPT_URL_LIST);
                Collection<String> allScripts = new MutableList<String>().appendIfNotNull(setupScript).appendAll(setupScripts);
                for (String setupScriptItem : allScripts) {
                    if (Strings.isNonBlank(setupScriptItem)) {
                        customisationForLogging.add("custom setup script " + setupScriptItem);

                        String setupVarsString = setup.get(JcloudsLocationConfig.CUSTOM_MACHINE_SETUP_SCRIPT_VARS);
                        Map<String, String> substitutions = (setupVarsString != null)
                                ? Splitter.on(",").withKeyValueSeparator(":").split(setupVarsString)
                                : ImmutableMap.<String, String>of();
                        String scriptContent = ResourceUtils.create(this).getResourceAsString(setupScriptItem);
                        String script = TemplateProcessor.processTemplateContents(scriptContent, getManagementContext(), substitutions);
                        if (windows) {
                            WinRmToolResponse resp = ((WinRmMachineLocation)machineLocation).executeCommand(ImmutableList.copyOf((script.replace("\r", "").split("\n"))));
                            if (resp.getStatusCode() != 0) {
                                throw new IllegalStateException("Command 'Customizing node " + this + "' failed with exit code " + resp.getStatusCode() + " for location " + machineLocation);
                            }
                        } else {
                            executeCommandThrowingOnError(
                                    (SshMachineLocation)machineLocation,
                                    "Customizing node " + this,
                                    ImmutableList.of(script));
                        }
                    }
                }

                Boolean dontRequireTtyForSudo = setup.get(JcloudsLocationConfig.DONT_REQUIRE_TTY_FOR_SUDO);
                if (Boolean.TRUE.equals(dontRequireTtyForSudo) ||
                        (dontRequireTtyForSudo == null && setup.get(DONT_CREATE_USER))) {
                    if (windows) {
                        LOG.warn("Ignoring flag DONT_REQUIRE_TTY_FOR_SUDO on Windows location {}", machineLocation);
                    } else {
                        customisationForLogging.add("patch /etc/sudoers to disable requiretty");

                        queueLocationTask("patch /etc/sudoers to disable requiretty",
                                SshTasks.dontRequireTtyForSudo((SshMachineLocation)machineLocation, true).newTask().asTask());
                    }
                }

                if (setup.get(JcloudsLocationConfig.MAP_DEV_RANDOM_TO_DEV_URANDOM)) {
                    if (windows) {
                        LOG.warn("Ignoring flag MAP_DEV_RANDOM_TO_DEV_URANDOM on Windows location {}", machineLocation);
                    } else {
                        customisationForLogging.add("point /dev/random to urandom");

                        executeCommandThrowingOnError(
                                (SshMachineLocation)machineLocation,
                                "using urandom instead of random",
                                Arrays.asList(
                                        BashCommands.sudo("mv /dev/random /dev/random-real"),
                                        BashCommands.sudo("ln -s /dev/urandom /dev/random")));
                    }
                }

                if (setup.get(GENERATE_HOSTNAME)) {
                    if (windows) {
                        // TODO: Generate Windows Hostname
                        LOG.warn("Ignoring flag GENERATE_HOSTNAME on Windows location {}", machineLocation);
                    } else {
                        customisationForLogging.add("configure hostname");

                        // also see TODO in SetHostnameCustomizer - ideally we share code between here and there
                        executeCommandThrowingOnError(
                                (SshMachineLocation)machineLocation,
                                "Generate hostname " + node.getName(),
                                ImmutableList.of(BashCommands.chainGroup(
                                        String.format("echo '127.0.0.1 %s' | ( %s )", node.getName(), BashCommands.sudo("tee -a /etc/hosts")),
                                        "{ " + BashCommands.sudo("sed -i \"s/HOSTNAME=.*/HOSTNAME=" + node.getName() + "/g\" /etc/sysconfig/network") + " || true ; }",
                                        BashCommands.sudo("hostname " + node.getName()))));
                    }
                }

                if (setup.get(OPEN_IPTABLES)) {
                    if (windows) {
                        LOG.warn("Ignoring DEPRECATED flag OPEN_IPTABLES on Windows location {}", machineLocation);
                    } else {
                        LOG.warn("Using DEPRECATED flag OPEN_IPTABLES (will not be supported in future versions) for {} at {}", machineLocation, this);

                        @SuppressWarnings("unchecked")
                        Iterable<Integer> inboundPorts = (Iterable<Integer>) setup.get(INBOUND_PORTS);

                        if (inboundPorts == null || Iterables.isEmpty(inboundPorts)) {
                            LOG.info("No ports to open in iptables (no inbound ports) for {} at {}", machineLocation, this);
                        } else {
                            customisationForLogging.add("open iptables");

                            List<String> iptablesRules = Lists.newArrayList();

                            if (isLocationFirewalldEnabled((SshMachineLocation)machineLocation)) {
                                for (Integer port : inboundPorts) {
                                    iptablesRules.add(IptablesCommands.addFirewalldRule(Chain.INPUT, Protocol.TCP, port, Policy.ACCEPT));
                                 }
                            } else {
                                iptablesRules = Lists.newArrayList();
                                for (Integer port : inboundPorts) {
                                   iptablesRules.add(IptablesCommands.insertIptablesRule(Chain.INPUT, Protocol.TCP, port, Policy.ACCEPT));
                                }
                                iptablesRules.add(IptablesCommands.saveIptablesRules());
                            }
                            List<String> batch = Lists.newArrayList();
                            // Some entities, such as Riak (erlang based) have a huge range of ports, which leads to a script that
                            // is too large to run (fails with a broken pipe). Batch the rules into batches of 50
                            for (String rule : iptablesRules) {
                                batch.add(rule);
                                if (batch.size() == 50) {
                                    executeCommandWarningOnError(
                                            (SshMachineLocation)machineLocation,
                                            "Inserting iptables rules, 50 command batch",
                                            batch);
                                    batch.clear();
                                }
                            }
                            if (batch.size() > 0) {
                                executeCommandWarningOnError(
                                        (SshMachineLocation)machineLocation,
                                        "Inserting iptables rules",
                                        batch);
                            }
                            executeCommandWarningOnError(
                                    (SshMachineLocation)machineLocation,
                                    "List iptables rules",
                                    ImmutableList.of(IptablesCommands.listIptablesRule()));
                        }
                    }
                }

                if (setup.get(STOP_IPTABLES)) {
                    if (windows) {
                        LOG.warn("Ignoring DEPRECATED flag OPEN_IPTABLES on Windows location {}", machineLocation);
                    } else {
                        LOG.warn("Using DEPRECATED flag STOP_IPTABLES (will not be supported in future versions) for {} at {}", machineLocation, this);

                        customisationForLogging.add("stop iptables");

                        List<String> cmds = ImmutableList.<String>of();
                        if (isLocationFirewalldEnabled((SshMachineLocation)machineLocation)) {
                            cmds = ImmutableList.of(IptablesCommands.firewalldServiceStop(), IptablesCommands.firewalldServiceStatus());
                        } else {
                            cmds = ImmutableList.of(IptablesCommands.iptablesServiceStop(), IptablesCommands.iptablesServiceStatus());
                        }
                        executeCommandWarningOnError(
                                (SshMachineLocation)machineLocation,
                                "Stopping iptables", cmds);
                    }
                }

                List<String> extraKeyUrlsToAuth = setup.get(EXTRA_PUBLIC_KEY_URLS_TO_AUTH);
                if (extraKeyUrlsToAuth!=null && !extraKeyUrlsToAuth.isEmpty()) {
                    if (windows) {
                        LOG.warn("Ignoring flag EXTRA_PUBLIC_KEY_URLS_TO_AUTH on Windows location", machineLocation);
                    } else {
                        List<String> extraKeyDataToAuth = MutableList.of();
                        for (String keyUrl : extraKeyUrlsToAuth) {
                            extraKeyDataToAuth.add(ResourceUtils.create().getResourceAsString(keyUrl));
                        }
                        executeCommandThrowingOnError(
                                (SshMachineLocation)machineLocation,
                                "Authorizing ssh keys from URLs",
                                ImmutableList.of(new AuthorizeRSAPublicKeys(extraKeyDataToAuth).render(org.jclouds.scriptbuilder.domain.OsFamily.UNIX)));
                    }
                }

                String extraKeyDataToAuth = setup.get(EXTRA_PUBLIC_KEY_DATA_TO_AUTH);
                if (extraKeyDataToAuth!=null && !extraKeyDataToAuth.isEmpty()) {
                    if (windows) {
                        LOG.warn("Ignoring flag EXTRA_PUBLIC_KEY_DATA_TO_AUTH on Windows location", machineLocation);
                    } else {
                        executeCommandThrowingOnError(
                                (SshMachineLocation)machineLocation,
                                "Authorizing ssh keys from data",
                                ImmutableList.of(new AuthorizeRSAPublicKeys(Collections.singletonList(extraKeyDataToAuth)).render(org.jclouds.scriptbuilder.domain.OsFamily.UNIX)));
                    }
                }

            } else {
                // Otherwise we have deliberately not waited to be ssh'able, so don't try now to
                // ssh to exec these commands!
            }

            // Apply any optional app-specific customization.
            for (JcloudsLocationCustomizer customizer : customizers) {
                LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
                customizer.customize(this, computeService, machineLocation);
            }
            for (MachineLocationCustomizer customizer : machineCustomizers) {
                LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
                customizer.customize(machineLocation);
            }

            customizedTimestamp = Duration.of(provisioningStopwatch);
            String logMessage = "Finished VM "+getCreationString(setup)+" creation:"
                    + " "+machineLocation.getUser()+"@"+machineLocation.getAddress()+":"+machineLocation.getPort()
                    + (Boolean.TRUE.equals(setup.get(LOG_CREDENTIALS))
                            ? "password=" + userCredentials.getOptionalPassword().or("<absent>")
                            + " && key=" + userCredentials.getOptionalPrivateKey().or("<absent>")
                            : "")
                    + " ready after "+Duration.of(provisioningStopwatch).toStringRounded()
                    + " ("
                    + "semaphore obtained in "+Duration.of(semaphoreTimestamp).toStringRounded()+";"
                    + template+" template built in "+Duration.of(templateTimestamp).subtract(semaphoreTimestamp).toStringRounded()+";"
                    + " "+node+" provisioned in "+Duration.of(provisionTimestamp).subtract(templateTimestamp).toStringRounded()+";"
                    + " "+machineLocation+" connection usable in "+Duration.of(usableTimestamp).subtract(provisionTimestamp).toStringRounded()+";"
                    + " and os customized in "+Duration.of(customizedTimestamp).subtract(usableTimestamp).toStringRounded()+" - "+Joiner.on(", ").join(customisationForLogging)+")";
            LOG.info(logMessage);

            return machineLocation;

        } catch (Exception e) {
            if (e instanceof RunNodesException && ((RunNodesException)e).getNodeErrors().size() > 0) {
                node = Iterables.get(((RunNodesException)e).getNodeErrors().keySet(), 0);
            }
            // sometimes AWS nodes come up busted (eg ssh not allowed); just throw it back (and maybe try for another one)
            boolean destroyNode = (node != null) && Boolean.TRUE.equals(setup.get(DESTROY_ON_FAILURE));

            if (e.toString().contains("VPCResourceNotSpecified")) {
                String message = "Detected that your EC2 account is a legacy 'EC2 Classic' account, "
                    + "but the most appropriate hardware instance type requires 'VPC'. "
                    + "One quick fix is to use the 'eu-central-1' region. "
                    + "Other remedies are described at "
                    + AWS_VPC_HELP_URL;
                LOG.error(message);
                e = new UserFacingException(message, e);
            }

            LOG.error("Failed to start VM for "+getCreationString(setup) + (destroyNode ? " (destroying)" : "")
                    + (node != null ? "; node "+node : "")
                    + " after "+Duration.of(provisioningStopwatch).toStringRounded()
                    + (semaphoreTimestamp != null ? " ("
                            + "semaphore obtained in "+Duration.of(semaphoreTimestamp).toStringRounded()+";"
                            + (templateTimestamp != null && semaphoreTimestamp != null ? " template built in "+Duration.of(templateTimestamp).subtract(semaphoreTimestamp).toStringRounded()+";" : "")
                            + (provisionTimestamp != null && templateTimestamp != null ? " node provisioned in "+Duration.of(provisionTimestamp).subtract(templateTimestamp).toStringRounded()+";" : "")
                            + (usableTimestamp != null && provisioningStopwatch != null ? " connection usable in "+Duration.of(usableTimestamp).subtract(provisionTimestamp).toStringRounded()+";" : "")
                            + (customizedTimestamp != null && usableTimestamp != null ? " and OS customized in "+Duration.of(customizedTimestamp).subtract(usableTimestamp).toStringRounded() : "")
                            + ")"
                            : "")
                    + ": "+e.getMessage());
            LOG.debug(Throwables.getStackTraceAsString(e));

            if (destroyNode) {
                Stopwatch destroyingStopwatch = Stopwatch.createStarted();
                if (machineLocation != null) {
                    releaseSafely(machineLocation);
                } else {
                    releaseNodeSafely(node);
                }
                LOG.info("Destroyed " + (machineLocation != null ? "machine " + machineLocation : "node " + node)
                        + " in " + Duration.of(destroyingStopwatch).toStringRounded());
            }

            throw Exceptions.propagate(e);
        }
    }

    private void executeCommandThrowingOnError(SshMachineLocation loc, String name, List<String> commands) {
        executeCommandThrowingOnError(ImmutableMap.<String, Object>of(), loc, name, commands);
    }

    private void executeCommandThrowingOnError(Map<String, Object> flags, SshMachineLocation loc, String name, List<String> commands) {
        Task<Integer> task = SshTasks.newSshExecTaskFactory(loc, commands)
            .summary(name)
            .requiringExitCodeZero()
            .configure(flags)
            .newTask()
            .asTask();
        queueLocationTask("waiting for '" + name + "' on machine " + loc, task);
    }

    protected <T> T queueLocationTask(String msg, Task<T> task) {
        TaskQueueingResult<T> queueResult = DynamicTasks.queueIfPossible(task);
        final String origDetails = Tasks.setBlockingDetails(msg);
        try {
            if(queueResult.isQueuedOrSubmitted()){
                return task.getUnchecked();
            } else {
                // TODO Should we add an `orExecuteInSameThread()` in `TaskQueueingResult`?
                try {
                    return ((TaskInternal<T>)task).getJob().call();
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }
        } finally {
            Tasks.setBlockingDetails(origDetails);
        }
    }

    private void executeCommandWarningOnError(SshMachineLocation loc, String name, List<String> commands) {
        Task<Integer> task = SshTasks.newSshExecTaskFactory(loc, commands)
            .summary(name)
            .allowingNonZeroExitCode()
            .newTask()
            .asTask();
        int ret = queueLocationTask("waiting for '" + name + "' on machine " + loc, task);
        if (ret != 0) {
            LOG.warn("Command '{}' failed with exit code {} for location {}", new Object[] {name, ret, this});
        }
    }

    // ------------- suspend and resume ------------------------------------

    private void putIfPresentButDifferent(ConfigBag setup, ConfigKey<String> key, String expectedValue) {
        if (expectedValue==null) return;
        String currentValue = setup.get(key);
        if (Objects.equal(currentValue, expectedValue)) {
            // no need to write -- and good reason not to --
            // the currentValue may come from an external supplier,
            // so we prefer to keep the secret in that supplier
            return;
        }
        // either current value is null, or
        // current value is different (possibly password coming from a one-time source)
        // in either case prefer the expected value
        setup.put(key, expectedValue);
    }

    /**
     * Suspends the given location.
     * <p>
     * Note that this method does <b>not</b> call the lifecycle methods of any
     * {@link #getCustomizers(ConfigBag) customizers} attached to this location.
     */
    @Override
    public void suspendMachine(MachineLocation rawLocation) {
        String instanceId = vmInstanceIds.remove(rawLocation);
        if (instanceId == null) {
            LOG.info("Attempt to suspend unknown machine " + rawLocation + " in " + this);
            throw new IllegalArgumentException("Unknown machine " + rawLocation);
        }
        LOG.info("Suspending machine {} in {}, instance id {}", new Object[]{rawLocation, this, instanceId});
        Exception toThrow = null;
        try {
            getComputeService().suspendNode(instanceId);
        } catch (Exception e) {
            toThrow = e;
            LOG.error("Problem suspending machine " + rawLocation + " in " + this + ", instance id " + instanceId, e);
        }
        removeChild(rawLocation);
        if (toThrow != null) {
            throw Exceptions.propagate(toThrow);
        }
    }

    /**
     * Brings an existing machine with the given details under management.
     * <p/>
     * Note that this method does <b>not</b> call the lifecycle methods of any
     * {@link #getCustomizers(ConfigBag) customizers} attached to this location.
     *
     * @param flags See {@link #registerMachine(ConfigBag)} for a description of required fields.
     * @see #registerMachine(ConfigBag)
     */
    @Override
    public JcloudsMachineLocation resumeMachine(Map<?, ?> flags) {
        ConfigBag setup = ConfigBag.newInstanceExtending(config().getBag(), flags);
        LOG.info("{} using resuming node matching properties: {}", this, Sanitizer.sanitize(setup));
        ComputeService computeService = getComputeService(setup);
        NodeMetadata node = findNodeOrThrow(setup);
        LOG.debug("{} resuming {}", this, node);
        computeService.resumeNode(node.getId());
        // Load the node a second time once it is resumed to get an object with
        // hostname and addresses populated.
        node = findNodeOrThrow(setup);
        LOG.debug("{} resumed {}", this, node);
        JcloudsMachineLocation registered = registerMachineLocation(setup, node);
        LOG.info("{} resumed and registered {}", this, registered);
        return registered;
    }

    // ------------- constructing the template, etc ------------------------

    /** @deprecated since 0.11.0 use {@link TemplateOptionCustomizer} instead */
    @Deprecated
    public interface CustomizeTemplateOptions extends TemplateOptionCustomizer {
    }

    /** properties which cause customization of the TemplateBuilder */
    public static final Map<ConfigKey<?>, ? extends TemplateBuilderCustomizer> SUPPORTED_TEMPLATE_BUILDER_PROPERTIES = ImmutableMap.<ConfigKey<?>, TemplateBuilderCustomizer>builder()
            .put(HARDWARE_ID, TemplateBuilderCustomizers.hardwareId())
            .put(IMAGE_DESCRIPTION_REGEX, TemplateBuilderCustomizers.imageDescription())
            .put(IMAGE_ID, TemplateBuilderCustomizers.imageId())
            .put(IMAGE_NAME_REGEX, TemplateBuilderCustomizers.imageNameRegex())
            .put(MIN_CORES, TemplateBuilderCustomizers.minCores())
            .put(MIN_DISK, TemplateBuilderCustomizers.minDisk())
            .put(MIN_RAM, TemplateBuilderCustomizers.minRam())
            .put(OS_64_BIT, TemplateBuilderCustomizers.os64Bit())
            .put(OS_FAMILY, TemplateBuilderCustomizers.osFamily())
            .put(OS_VERSION_REGEX, TemplateBuilderCustomizers.osVersionRegex())
            .put(TEMPLATE_SPEC, TemplateBuilderCustomizers.templateSpec())
            /* Both done in the code, but included here so that they are in the map */
            .put(DEFAULT_IMAGE_ID, TemplateBuilderCustomizers.noOp())
            .put(TEMPLATE_BUILDER, TemplateBuilderCustomizers.noOp())
            .build();

    /** properties which cause customization of the TemplateOptions */
    public static final Map<ConfigKey<?>, ? extends TemplateOptionCustomizer>SUPPORTED_TEMPLATE_OPTIONS_PROPERTIES = ImmutableMap.<ConfigKey<?>, TemplateOptionCustomizer>builder()
            .put(AUTO_ASSIGN_FLOATING_IP, TemplateOptionCustomizers.autoAssignFloatingIp())
            .put(AUTO_CREATE_FLOATING_IPS, TemplateOptionCustomizers.autoCreateFloatingIps())
            .put(AUTO_GENERATE_KEYPAIRS, TemplateOptionCustomizers.autoGenerateKeypairs())
            .put(DOMAIN_NAME, TemplateOptionCustomizers.domainName())
            .put(EXTRA_PUBLIC_KEY_DATA_TO_AUTH, TemplateOptionCustomizers.extraPublicKeyDataToAuth())
            .put(INBOUND_PORTS, TemplateOptionCustomizers.inboundPorts())
            .put(KEY_PAIR, TemplateOptionCustomizers.keyPair())
            .put(LOGIN_USER, TemplateOptionCustomizers.loginUser())
            .put(LOGIN_USER_PASSWORD, TemplateOptionCustomizers.loginUserPassword())
            .put(LOGIN_USER_PRIVATE_KEY_DATA, TemplateOptionCustomizers.loginUserPrivateKeyData())
            .put(LOGIN_USER_PRIVATE_KEY_FILE, TemplateOptionCustomizers.loginUserPrivateKeyFile())
            .put(NETWORK_NAME, TemplateOptionCustomizers.networkName())
            .put(RUN_AS_ROOT, TemplateOptionCustomizers.runAsRoot())
            .put(SECURITY_GROUPS, TemplateOptionCustomizers.securityGroups())
            .put(STRING_TAGS, TemplateOptionCustomizers.stringTags())
            .put(TEMPLATE_OPTIONS, TemplateOptionCustomizers.templateOptions())
            .put(USER_DATA_UUENCODED, TemplateOptionCustomizers.userDataUuencoded())
            .put(USER_METADATA_MAP, TemplateOptionCustomizers.userMetadataMap())
            .put(USER_METADATA_STRING, TemplateOptionCustomizers.userMetadataString())
            .build();

    /** hook whereby template customizations can be made for various clouds */
    protected void customizeTemplate(ComputeService computeService, Template template, Collection<JcloudsLocationCustomizer> customizers) {
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(this, computeService, template);
            customizer.customize(this, computeService, template.getOptions());
        }

        // these things are nice on softlayer
        if (template.getOptions() instanceof SoftLayerTemplateOptions) {
            SoftLayerTemplateOptions slT = ((SoftLayerTemplateOptions)template.getOptions());
            if (Strings.isBlank(slT.getDomainName()) || "jclouds.org".equals(slT.getDomainName())) {
                // set a quasi-sensible domain name if none was provided (better than the default, jclouds.org)
                // NB: things like brooklyn.local are disallowed
                slT.domainName("local.brooklyncentral.org");
            }
            // convert user metadata to tags and notes because user metadata is otherwise ignored
            Map<String, String> md = slT.getUserMetadata();
            if (md!=null && !md.isEmpty()) {
                Set<String> tags = MutableSet.copyOf(slT.getTags());
                for (Map.Entry<String,String> entry: md.entrySet()) {
                    tags.add(AbstractCloudMachineNamer.sanitize(entry.getKey())+":"+AbstractCloudMachineNamer.sanitize(entry.getValue()));
                }
                slT.tags(tags);

                if (!md.containsKey("notes")) {
                    String notes = "User Metadata\n=============\n\n  * " + Joiner.on("\n  * ").withKeyValueSeparator(": ").join(md);
                    if (notes.length() > NOTES_MAX_LENGTH) {
                        String truncatedMsg = "...\n<truncated - notes total length is " + notes.length() + " characters>";
                        notes = notes.substring(0, NOTES_MAX_LENGTH - truncatedMsg.length()) + truncatedMsg;
                    }
                    md.put("notes", notes);
                }
            }
        }
    }

    /**
     * If the ImageChooser is a string, then try instantiating a class with that name (in the same
     * way as we do for {@link #getCloudMachineNamer(ConfigBag)}, for example). Otherwise, assume
     * that convention TypeCoercions will work.
     */
    @SuppressWarnings("unchecked")
    protected Function<Iterable<? extends Image>, Image> getImageChooser(ComputeService computeService, ConfigBag config) {
        Function<Iterable<? extends Image>, Image> chooser;
        Object rawVal = config.getStringKey(JcloudsLocationConfig.IMAGE_CHOOSER.getName());
        if (rawVal instanceof String && Strings.isNonBlank((String)rawVal)) {
            // Configured with a string: it could be a class that we need to instantiate
            Class<?> clazz;
            try {
                clazz = new ClassLoaderUtils(this.getClass(), getManagementContext()).loadClass((String)rawVal);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not load configured ImageChooser " + rawVal, e);
            }
            Maybe<?> instance = Reflections.invokeConstructorFromArgs(clazz);
            if (!instance.isPresent()) {
                throw new IllegalStateException("Failed to create ImageChooser "+rawVal+" for location "+this);
            } else if (!(instance.get() instanceof Function)) {
                throw new IllegalStateException("Failed to create ImageChooser "+rawVal+" for location "+this+"; expected type Function but got "+instance.get().getClass());
            } else {
                chooser = (Function<Iterable<? extends Image>, Image>) instance.get();
            }
        } else {
            chooser = config.get(JcloudsLocationConfig.IMAGE_CHOOSER);
        }
        return BrooklynImageChooser.cloneFor(chooser, computeService, config);
    }

    /** returns the jclouds Template which describes the image to be built, for the given config and compute service */
    public Template buildTemplate(ComputeService computeService, ConfigBag config, Collection<JcloudsLocationCustomizer> customizers) {
        TemplateBuilder templateBuilder = config.get(TEMPLATE_BUILDER);
        if (templateBuilder==null) {
            templateBuilder = new PortableTemplateBuilder<PortableTemplateBuilder<?>>();
        } else {
            LOG.debug("jclouds using templateBuilder {} as custom base for provisioning in {} for {}", new Object[] {
                    templateBuilder, this, getCreationString(config)});
        }
        if (templateBuilder instanceof PortableTemplateBuilder<?>) {
            if (((PortableTemplateBuilder<?>)templateBuilder).imageChooser()==null) {
                Function<Iterable<? extends Image>, Image> chooser = getImageChooser(computeService, config);
                templateBuilder.imageChooser(chooser);
            } else {
                // an image chooser is already set, so do nothing
            }
        } else {
            // template builder supplied, and we cannot check image chooser status; warn, for now
            LOG.warn("Cannot check imageChooser status for {} due to manually supplied black-box TemplateBuilder; "
                + "it is recommended to use a PortableTemplateBuilder if you supply a TemplateBuilder", getCreationString(config));
        }

        if (!Strings.isEmpty(config.get(CLOUD_REGION_ID))) {
            templateBuilder.locationId(config.get(CLOUD_REGION_ID));
        }

        if (Strings.isNonBlank(config.get(HARDWARE_ID))) {
            String oldHardwareId = config.get(HARDWARE_ID);
            String newHardwareId = transformHardwareId(oldHardwareId, config);
            if (!Objects.equal(oldHardwareId, newHardwareId)) {
                LOG.info("Transforming hardwareId from " + oldHardwareId + " to " + newHardwareId + ", in " + toString());
                config.put(HARDWARE_ID, newHardwareId);
            }
        }

        // Apply the template builder and options properties
        for (Map.Entry<ConfigKey<?>, ? extends TemplateBuilderCustomizer> entry : SUPPORTED_TEMPLATE_BUILDER_PROPERTIES.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            Object val = config.containsKey(key) ? config.get(key) : key.getDefaultValue();
            if (val != null) {
                TemplateBuilderCustomizer code = entry.getValue();
                code.apply(templateBuilder, config, val);
            }
        }

        if (templateBuilder instanceof PortableTemplateBuilder) {
            ((PortableTemplateBuilder<?>)templateBuilder).attachComputeService(computeService);
            // do the default last, and only if nothing else specified (guaranteed to be a PTB if nothing else specified)
            if (groovyTruth(config.get(DEFAULT_IMAGE_ID))) {
                if (((PortableTemplateBuilder<?>)templateBuilder).isBlank()) {
                    templateBuilder.imageId(config.get(DEFAULT_IMAGE_ID).toString());
                }
            }
        }

        // Then apply any optional app-specific customization.
        for (JcloudsLocationCustomizer customizer : customizers) {
            customizer.customize(this, computeService, templateBuilder);
        }

        LOG.debug("jclouds using templateBuilder {} for provisioning in {} for {}", new Object[] {
            templateBuilder, this, getCreationString(config)});

        // Finally try to build the template
        Template template = null;
        Image image;
        try {
            template = templateBuilder.build();
            if (template==null) throw new IllegalStateException("No matching template; check image and hardware constraints (e.g. OS, RAM); using "+templateBuilder);
            image = template.getImage();
            LOG.debug("jclouds found template "+template+" (image "+image+") for provisioning in "+this+" for "+getCreationString(config));
            if (image==null) throw new IllegalStateException("No matching image in template at "+toStringNice()+"; check image constraints (OS, providers, ID); using "+templateBuilder);
        } catch (AuthorizationException e) {
            LOG.warn("Error resolving template -- not authorized (rethrowing: "+e+"); template is: "+template);
            throw new IllegalStateException("Not authorized to access cloud "+toStringNice()+"; "+
                "check identity, credentials, and endpoint (identity='"+getIdentity()+"', credential length "+getCredential().length()+")", e);
        } catch (Exception e) {
            try {
                IOException ioe = Exceptions.getFirstThrowableOfType(e, IOException.class);
                if (ioe != null) {
                    LOG.warn("IOException found...", ioe);
                    throw ioe;
                }
                if (listedAvailableTemplatesOnNoSuchTemplate.compareAndSet(false, true)) {
                    // delay subsequent log.warns (put in synch block) so the "Loading..." message is obvious
                    LOG.warn("Unable to match required VM template constraints "+templateBuilder+" when trying to provision VM in "+this+" (rethrowing): "+e);
                    logAvailableTemplates(config);
                }
            } catch (Exception e2) {
                LOG.warn("Error loading available images to report (following original error matching template which will be rethrown): "+e2, e2);
                throw new IllegalStateException("Unable to access cloud "+this+" to resolve "+templateBuilder+": "+e, e);
            }
            throw new IllegalStateException("Unable to match required VM template constraints "+templateBuilder+" when trying to provision VM in "+this+"; "
                + "see list of images in log. Root cause: "+e, e);
        }
        TemplateOptions options = template.getOptions();

        // For windows, we need a startup-script to be executed that will enable winrm access.
        // If there is already conflicting userMetadata, then don't replace it (and just warn).
        // TODO this injection is hacky and (currently) cloud specific.
        boolean windows = isWindows(template, config);
        if (windows) {
            String initScript = WinRmMachineLocation.getDefaultUserMetadataString(config());
            String provider = getProvider();
            if ("google-compute-engine".equals(provider)) {
                // see https://cloud.google.com/compute/docs/startupscript:
                // Set "sysprep-specialize-script-cmd" in metadata.
                String startupScriptKey = "sysprep-specialize-script-cmd";
                Object metadataMapRaw = config.get(USER_METADATA_MAP);
                if (metadataMapRaw instanceof Map) {
                    Map<?,?> metadataMap = (Map<?, ?>) metadataMapRaw;
                    if (metadataMap.containsKey(startupScriptKey)) {
                        LOG.warn("Not adding startup-script for Windows VM on "+provider+", because already has key "+startupScriptKey+" in config "+USER_METADATA_MAP.getName());
                    } else {
                        Map<Object, Object> metadataMapReplacement = MutableMap.copyOf(metadataMap);
                        metadataMapReplacement.put(startupScriptKey, initScript);
                        config.put(USER_METADATA_MAP, metadataMapReplacement);
                        LOG.debug("Adding startup-script to enable WinRM for Windows VM on "+provider);
                    }
                } else if (metadataMapRaw == null) {
                    Map<String, String> metadataMapReplacement = MutableMap.of(startupScriptKey, initScript);
                    config.put(USER_METADATA_MAP, metadataMapReplacement);
                    LOG.debug("Adding startup-script to enable WinRM for Windows VM on "+provider);
                }
            } else {
                // For AWS and vCloudDirector, we just set user_metadata_string.
                // For Azure-classic, there is no capability to execute a startup script.
                boolean userMetadataString = config.containsKey(JcloudsLocationConfig.USER_METADATA_STRING);
                boolean userMetadataMap = config.containsKey(JcloudsLocationConfig.USER_METADATA_MAP);
                if (!(userMetadataString || userMetadataMap)) {
                    config.put(JcloudsLocationConfig.USER_METADATA_STRING, WinRmMachineLocation.getDefaultUserMetadataString(config()));
                    LOG.debug("Adding startup-script to enable WinRM for Windows VM on "+provider);
                } else {
                    LOG.warn("Not adding startup-script for Windows VM on "+provider+", because already has config "
                            +(userMetadataString ? USER_METADATA_STRING.getName() : USER_METADATA_MAP.getName()));
                }
            }
        }

        for (Map.Entry<ConfigKey<?>, ? extends TemplateOptionCustomizer> entry : SUPPORTED_TEMPLATE_OPTIONS_PROPERTIES.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            TemplateOptionCustomizer code = entry.getValue();
            if (config.containsKey(key) && config.get(key) != null) {
                code.apply(options, config, config.get(key));
            }
        }

        return template;
    }


    /**
     * See {@link https://issues.apache.org/jira/browse/JCLOUDS-1108}.
     * 
     * In jclouds 1.9.x and 2.0.0, google-compute-engine hardwareId must be in the long form. For 
     * example {@code https://www.googleapis.com/compute/v1/projects/jclouds-gce/zones/us-central1-a/machineTypes/n1-standard-1}.
     * It is much nicer to support the short-form (e.g. {@code n1-standard-1}), and to construct 
     * the long-form from this.
     * 
     * The "zone" in the long-form needs to match the region (see {@link #getRegion()}).
     * 
     * The ideal would be for jclouds to do this. But that isn't available yet - in the mean time,
     * we can make life easier for our users with the code below.
     * 
     * Second best would have been handling this in {@link TemplateBuilderCustomizers#hardwareId()}. 
     * However, that code doesn't have enough context to know what to do (easily!). It is passed
     * {@code apply(TemplateBuilder tb, ConfigBag props, Object v)}, so doesn't even know which 
     * provider it is being called for (without doing ugly/brittle digging in the {@code props}
     * that it is given).
     * 
     * Therefore we do the transform here.
     */
    private String transformHardwareId(String hardwareId, ConfigBag config) {
        checkNotNull(hardwareId, "hardwareId");
        checkNotNull(config, "config");
        
        String provider = getProvider();
        String region = getRegion();
        if (Strings.isBlank(region)) region = config.get(CLOUD_REGION_ID);
        
        if (!"google-compute-engine".equals(provider)) {
            return hardwareId;
        }
        if (hardwareId.toLowerCase().startsWith("http") || hardwareId.contains("/")) {
            // looks like it's already in long-form: don't transform
            return hardwareId;
        }
        if (Strings.isNonBlank(region)) {
            return String.format("https://www.googleapis.com/compute/v1/projects/jclouds-gce/zones/%s/machineTypes/%s", region, hardwareId);
        } else {
            LOG.warn("Cannot transform GCE hardwareId (" + hardwareId + ") to long form, because region unknown in " + toString());
            return hardwareId;
        }
    }

    protected String toStringNice() {
        String s = config().get(ORIGINAL_SPEC);
        if (Strings.isBlank(s)) s = config().get(NAMED_SPEC_NAME);
        if (Strings.isBlank(s)) s = config().get(FINAL_SPEC);
        if (Strings.isBlank(s)) s = getDisplayName();

        String s2 = "";
        String provider = getProvider();
        if (Strings.isBlank(s) || (Strings.isNonBlank(provider) && !s.toLowerCase().contains(provider.toLowerCase())))
            s2 += " "+provider;
        String region = getRegion();
        if (Strings.isBlank(s) || (Strings.isNonBlank(region) && !s.toLowerCase().contains(region.toLowerCase())))
            s2 += " "+region;
        String endpoint = getEndpoint();
        if (Strings.isBlank(s) || (Strings.isNonBlank(endpoint) && !s.toLowerCase().contains(endpoint.toLowerCase())))
            s2 += " "+endpoint;
        s2 = s2.trim();
        if (Strings.isNonBlank(s)) {
            if (Strings.isNonBlank(s2)) {
                return s+" ("+s2+")";
            }
            return s;
        }
        if (Strings.isNonBlank(s2)) {
            return s2;
        }
        // things are bad if we get to this point!
        return toString();
    }

    protected void logAvailableTemplates(ConfigBag config) {
        LOG.info("Loading available images at "+this+" for reference...");
        ConfigBag m1 = ConfigBag.newInstanceCopying(config);
        if (m1.containsKey(IMAGE_ID)) {
            // if caller specified an image ID, remove that, but don't apply default filters
            m1.remove(IMAGE_ID);
            // TODO use key
            m1.putStringKey("anyOwner", true);
        }
        ComputeService computeServiceLessRestrictive = getComputeService(m1);
        Set<? extends Image> imgs = computeServiceLessRestrictive.listImages();
        LOG.info(""+imgs.size()+" available images at "+this);
        for (Image img: imgs) {
            LOG.info(" Image: "+img);
        }

        Set<? extends Hardware> profiles = computeServiceLessRestrictive.listHardwareProfiles();
        LOG.info(""+profiles.size()+" available profiles at "+this);
        for (Hardware profile: profiles) {
            LOG.info(" Profile: "+profile);
        }

        Set<? extends org.jclouds.domain.Location> assignableLocations = computeServiceLessRestrictive.listAssignableLocations();
        LOG.info(""+assignableLocations.size()+" available locations at "+this);
        for (org.jclouds.domain.Location assignableLocation: assignableLocations) {
            LOG.info(" Location: "+assignableLocation);
        }
    }

    /**
     * Creates a temporary ssh machine location (i.e. will not be persisted), which uses the given credentials.
     * It ignores any credentials (e.g. password, key-phrase, etc) that are supplied in the config.
     */
    protected SshMachineLocation createTemporarySshMachineLocation(HostAndPort hostAndPort, LoginCredentials creds, ConfigBag config) {
        String initialUser = creds.getUser();
        Optional<String> initialPassword = creds.getOptionalPassword();
        Optional<String> initialPrivateKey = creds.getOptionalPrivateKey();

        Map<String,Object> sshProps = Maps.newLinkedHashMap(config.getAllConfig());
        sshProps.put("user", initialUser);
        sshProps.put("address", hostAndPort.getHostText());
        sshProps.put("port", hostAndPort.getPort());
        sshProps.put(AbstractLocation.TEMPORARY_LOCATION.getName(), true);
        sshProps.put(LocalLocationManager.CREATE_UNMANAGED.getName(), true);
        String sshClass = config().get(SshMachineLocation.SSH_TOOL_CLASS);
        if (Strings.isNonBlank(sshClass)) {
            sshProps.put(SshMachineLocation.SSH_TOOL_CLASS.getName(), sshClass);
        }

        sshProps.remove("id");
        sshProps.remove("password");
        sshProps.remove("privateKeyData");
        sshProps.remove("privateKeyFile");
        sshProps.remove("privateKeyPassphrase");

        if (initialPassword.isPresent()) sshProps.put("password", initialPassword.get());
        if (initialPrivateKey.isPresent()) sshProps.put("privateKeyData", initialPrivateKey.get());

        if (isManaged()) {
            return getManagementContext().getLocationManager().createLocation(sshProps, SshMachineLocation.class);
        } else {
            return new SshMachineLocation(sshProps);
        }
    }

    /**
     * Creates a temporary WinRM machine location (i.e. will not be persisted), which uses the given credentials.
     * It ignores any credentials (e.g. password, key-phrase, etc) that are supplied in the config.
     */
    protected WinRmMachineLocation createTemporaryWinRmMachineLocation(HostAndPort hostAndPort, LoginCredentials creds, ConfigBag config) {
        String initialUser = creds.getUser();
        Optional<String> initialPassword = creds.getOptionalPassword();
        Optional<String> initialPrivateKey = creds.getOptionalPrivateKey();

        Map<String,Object> winrmProps = Maps.newLinkedHashMap(config.getAllConfig());
        winrmProps.put("user", initialUser);
        winrmProps.put("address", hostAndPort.getHostText());
        winrmProps.put("port", hostAndPort.getPort());
        winrmProps.put(AbstractLocation.TEMPORARY_LOCATION.getName(), true);
        winrmProps.put(LocalLocationManager.CREATE_UNMANAGED.getName(), true);
        winrmProps.remove("password");
        winrmProps.remove("privateKeyData");
        winrmProps.remove("privateKeyFile");
        winrmProps.remove("privateKeyPassphrase");
        String winrmClass = config().get(WinRmMachineLocation.WINRM_TOOL_CLASS);
        if (Strings.isNonBlank(winrmClass)) {
            winrmProps.put(WinRmMachineLocation.WINRM_TOOL_CLASS.getName(), winrmClass);
        }

        if (initialPassword.isPresent()) winrmProps.put("password", initialPassword.get());
        if (initialPrivateKey.isPresent()) winrmProps.put("privateKeyData", initialPrivateKey.get());

        if (isManaged()) {
            return getManagementContext().getLocationManager().createLocation(winrmProps, WinRmMachineLocation.class);
        } else {
            throw new UnsupportedOperationException("Cannot create temporary WinRmMachineLocation because " + this + " is not managed");
        }
    }

    /**
     * Create the user immediately - executing ssh commands as required.
     */
    protected LoginCredentials createUser(
            ComputeService computeService, NodeMetadata node, HostAndPort managementHostAndPort,
            LoginCredentials initialCredentials, ConfigBag config) {
        Image image = (node.getImageId() != null) ? computeService.getImage(node.getImageId()) : null;
        CreateUserStatements userCreation = createUserStatements(image, config);

        if (!userCreation.statements().isEmpty()) {
            // If unsure of OS family, default to unix for rendering statements.
            org.jclouds.scriptbuilder.domain.OsFamily scriptOsFamily;
            if (isWindows(node, config)) {
                scriptOsFamily = org.jclouds.scriptbuilder.domain.OsFamily.WINDOWS;
            } else {
                scriptOsFamily = org.jclouds.scriptbuilder.domain.OsFamily.UNIX;
            }

            boolean windows = isWindows(node, config);

            if (windows) {
                LOG.warn("Unable to execute statements on WinRM in JcloudsLocation; skipping for "+node+": "+userCreation.statements());
            } else {
                List<String> commands = Lists.newArrayList();
                for (Statement statement : userCreation.statements()) {
                    InitAdminAccess initAdminAccess = new InitAdminAccess(new AdminAccessConfiguration.Default());
                    initAdminAccess.visit(statement);
                    commands.add(statement.render(scriptOsFamily));
                }

                String initialUser = initialCredentials.getUser();

                // TODO Retrying lots of times as workaround for vcloud-director. There the guest customizations
                // can cause the VM to reboot shortly after it was ssh'able.
                Map<String,Object> execProps = Maps.newLinkedHashMap();
                execProps.put(ShellTool.PROP_RUN_AS_ROOT.getName(), true);
                execProps.put(SshTool.PROP_ALLOCATE_PTY.getName(), true);
                execProps.put(SshTool.PROP_SSH_TRIES.getName(), 50);
                execProps.put(SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), 10*60*1000);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("VM {}: executing user creation/setup via {}@{}; commands: {}", new Object[] {
                            getCreationString(config), initialUser, managementHostAndPort, commands});
                }

                SshMachineLocation sshLoc = createTemporarySshMachineLocation(managementHostAndPort, initialCredentials, config);
                try {
                    // BROOKLYN-188: for SUSE, need to specify the path (for groupadd, useradd, etc)
                    Map<String, ?> env = ImmutableMap.of("PATH", sbinPath());

                    int exitcode = sshLoc.execScript(execProps, "create-user", commands, env);

                    if (exitcode != 0) {
                        LOG.warn("exit code {} when creating user for {}; usage may subsequently fail", exitcode, node);
                    }
                } finally {
                    getManagementContext().getLocationManager().unmanage(sshLoc);
                    Streams.closeQuietly(sshLoc);
                }
            }
        }

        return userCreation.credentials();
    }

    /**
     * Set up the TemplateOptions to create the user.
     */
    protected LoginCredentials initTemplateForCreateUser(Template template, ConfigBag config) {
        CreateUserStatements userCreation = createUserStatements(template.getImage(), config);

        if (!userCreation.statements().isEmpty()) {
            TemplateOptions options = template.getOptions();
            options.runScript(new StatementList(userCreation.statements()));
        }

        return userCreation.credentials();
    }

    /** @deprecated since 0.11.0 use {@link CreateUserStatements} instead. */
    @Deprecated
    protected static class UserCreation extends CreateUserStatements  {
        public final LoginCredentials createdUserCredentials;
        public final List<Statement> statements;

        public UserCreation(LoginCredentials creds, List<Statement> statements) {
            super(creds, statements);
            this.createdUserCredentials = super.credentials();
            this.statements = super.statements();
        }
    }

    /** @deprecated since 0.11.0 return type will be changed to {@link CreateUserStatements} in a future release. */
    @Deprecated
    protected UserCreation createUserStatements(@Nullable Image image, ConfigBag config) {
        CreateUserStatements userStatements = CreateUserStatements.get(this, image, config);
        return new UserCreation(userStatements.credentials(), userStatements.statements());
    }

    // ----------------- registering existing machines ------------------------

    /**
     * @deprecated since 0.8.0 use {@link #registerMachine(NodeMetadata)} instead.
     */
    @Deprecated
    public JcloudsSshMachineLocation rebindMachine(NodeMetadata metadata) throws NoMachinesAvailableException {
        return (JcloudsSshMachineLocation) registerMachine(metadata);
    }

    protected MachineLocation registerMachine(NodeMetadata metadata) throws NoMachinesAvailableException {
        return registerMachine(MutableMap.of(), metadata);
    }

    /**
     * @deprecated since 0.8.0 use {@link #registerMachine(Map, NodeMetadata)} instead.
     */
    @Deprecated
    public JcloudsSshMachineLocation rebindMachine(Map<?,?> flags, NodeMetadata metadata) throws NoMachinesAvailableException {
        return (JcloudsSshMachineLocation) registerMachine(flags, metadata);
    }

    protected MachineLocation registerMachine(Map<?, ?> flags, NodeMetadata metadata) throws NoMachinesAvailableException {
        ConfigBag setup = ConfigBag.newInstanceExtending(config().getBag(), flags);
        if (!setup.containsKey("id")) setup.putStringKey("id", metadata.getId());
        setHostnameUpdatingCredentials(setup, metadata);
        return registerMachine(setup);
    }

    /**
     * Brings an existing machine with the given details under management.
     * <p>
     * This method will throw an exception if used to reconnect to a Windows VM.
     * @deprecated since 0.8.0 use {@link #registerMachine(ConfigBag)} instead.
     */
    @Deprecated
    public JcloudsSshMachineLocation rebindMachine(ConfigBag setup) throws NoMachinesAvailableException {
        return (JcloudsSshMachineLocation) registerMachine(setup);
    }

    /**
     * Brings an existing machine with the given details under management.
     * <p>
     * Required fields are:
     * <ul>
     *   <li>id: the jclouds VM id, e.g. "eu-west-1/i-5504f21d" (NB this is {@see JcloudsMachineLocation#getJcloudsId()} not #getId())
     *   <li>hostname: the public hostname or IP of the machine, e.g. "ec2-176-34-93-58.eu-west-1.compute.amazonaws.com"
     *   <li>userName: the username for sshing into the machine (for use if it is not a Windows system)
     * <ul>
     */
    public JcloudsMachineLocation registerMachine(ConfigBag setup) throws NoMachinesAvailableException {
        NodeMetadata node = findNodeOrThrow(setup);
        return registerMachineLocation(setup, node);
    }

    protected JcloudsMachineLocation registerMachineLocation(ConfigBag setup, NodeMetadata node) {
        ComputeService computeService = getComputeService(setup);
        boolean windows = isWindows(node, setup);

        // Not publishing networks since they should have previously been published.
        ConnectivityResolverOptions options = getConnectivityOptionsBuilder(setup, windows)
                .initialCredentials(node.getCredentials())
                .userCredentials(node.getCredentials())
                .defaultLoginPort(node.getLoginPort())
                .isRebinding(true)
                .build();
        HostAndPort managementHostAndPort = getLocationNetworkInfoCustomizer(setup)
                .resolve(this, node, setup, options)
                .hostAndPort();

        if (managementHostAndPort == null) {
            throw new IllegalStateException("Could not resolve management host and port for " + node + " given options: " + options);
        }

        if (windows) {
            return registerWinRmMachineLocation(computeService, node, Optional.<Template>absent(), node.getCredentials(), managementHostAndPort, setup);
        } else {
            try {
                return registerJcloudsSshMachineLocation(computeService, node, Optional.<Template>absent(), node.getCredentials(), managementHostAndPort, setup);
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    /**
     * Finds a node matching the properties given in config or throws an exception.
     * @param config
     * @return
     */
    protected NodeMetadata findNodeOrThrow(ConfigBag config) {
        String user = checkNotNull(getUser(config), "user");
        String rawId = (String) config.getStringKey("id");
        String rawHostname = (String) config.getStringKey("hostname");
        Predicate<ComputeMetadata> predicate = getRebindToMachinePredicate(config);
        LOG.debug("Finding VM {} ({}@{}), in jclouds location for provider {} matching {}", new Object[]{
                rawId != null ? rawId : "<lookup>",
                user,
                rawHostname != null ? rawHostname : "<unspecified>",
                getProvider(),
                predicate
        });
        ComputeService computeService = getComputeService(config);
        Set<? extends NodeMetadata> candidateNodes = computeService.listNodesDetailsMatching(predicate);
        if (candidateNodes.isEmpty()) {
            throw new IllegalArgumentException("Jclouds node not found for rebind with predicate " + predicate);
        } else if (candidateNodes.size() > 1) {
            throw new IllegalArgumentException("Jclouds node for rebind matched multiple with " + predicate + ": " + candidateNodes);
        }
        NodeMetadata node = Iterables.getOnlyElement(candidateNodes);

        OsCredential osCredentials = LocationConfigUtils.getOsCredential(config).checkNoErrors().logAnyWarnings();
        String pkd = osCredentials.getPrivateKeyData();
        String password = osCredentials.getPassword();
        LoginCredentials expectedCredentials = node.getCredentials();
        if (Strings.isNonBlank(pkd)) {
            expectedCredentials = LoginCredentials.fromCredentials(new Credentials(user, pkd));
        } else if (Strings.isNonBlank(password)) {
            expectedCredentials = LoginCredentials.fromCredentials(new Credentials(user, password));
        } else if (expectedCredentials == null) {
            //need some kind of credential object, or will get NPE later
            expectedCredentials = LoginCredentials.fromCredentials(new Credentials(user, null));
        }
        node = NodeMetadataBuilder.fromNodeMetadata(node).credentials(expectedCredentials).build();

        return node;
    }

    /**
     * @deprecated since 0.8.0 use {@link #registerMachine(Map)} instead.
     */
    @Deprecated
    public JcloudsSshMachineLocation rebindMachine(Map<?, ?> flags) throws NoMachinesAvailableException {
        return (JcloudsSshMachineLocation) registerMachine(flags);
    }

    public MachineLocation registerMachine(Map<?,?> flags) throws NoMachinesAvailableException {
        ConfigBag setup = ConfigBag.newInstanceExtending(config().getBag(), flags);
        return registerMachine(setup);
    }

    /**
     * @return a predicate that returns true if a {@link ComputeMetadata} instance is suitable for
     *      rebinding to given the configuration in {@link ConfigBag config}.
     */
    protected Predicate<ComputeMetadata> getRebindToMachinePredicate(ConfigBag config) {
        return new RebindToMachinePredicate(config);
    }

    protected JcloudsSshMachineLocation registerJcloudsSshMachineLocation(
            ComputeService computeService, NodeMetadata node, Optional<Template> template,
            LoginCredentials credentials, HostAndPort managementHostAndPort, ConfigBag setup) throws IOException {
        JcloudsSshMachineLocation machine = createJcloudsSshMachineLocation(computeService, node, template, credentials, managementHostAndPort, setup);
        registerJcloudsMachineLocation(node.getId(), machine);
        return machine;
    }

    @VisibleForTesting
    protected void registerJcloudsMachineLocation(String nodeId, JcloudsMachineLocation machine) {
        machine.setParent(this);
        vmInstanceIds.put(machine, nodeId);
    }

    protected JcloudsSshMachineLocation createJcloudsSshMachineLocation(
            ComputeService computeService, NodeMetadata node, Optional<Template> template,
            LoginCredentials userCredentials, HostAndPort managementHostAndPort, ConfigBag setup) throws IOException {

        Collection<JcloudsLocationCustomizer> customizers = getCustomizers(setup);
        Collection<MachineLocationCustomizer> machineCustomizers = getMachineCustomizers(setup);
        Map<?,?> sshConfig = extractSshConfig(setup, node);
        String nodeAvailabilityZone = extractAvailabilityZone(setup, node);
        String nodeRegion = extractRegion(setup, node);
        if (nodeRegion == null) {
            // e.g. rackspace doesn't have "region", so rackspace-uk is best we can say (but zone="LON")
            nodeRegion = extractProvider(setup, node);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("creating JcloudsSshMachineLocation representation for {}@{} ({}) for {}/{}",
                    new Object[]{
                            getUser(setup),
                            managementHostAndPort,
                            Sanitizer.sanitize(sshConfig),
                            getCreationString(setup),
                            node
                    });
        }

        String address = managementHostAndPort.getHostText();
        int port = managementHostAndPort.hasPort() ? managementHostAndPort.getPort() : node.getLoginPort();
        
        // The display name will be one of the IPs of the VM (preferring public if there are any).
        // If the managementHostAndPort matches any of the IP contenders, then prefer that.
        // (Don't just use the managementHostAndPort, because that could be using DNAT so could be
        // a shared IP address, for example).
        String displayName = getPublicHostnameGeneric(node, setup, Optional.of(address));
        
        final Object password = sshConfig.get(SshMachineLocation.PASSWORD.getName()) != null
                ? sshConfig.get(SshMachineLocation.PASSWORD.getName())
                : userCredentials.getOptionalPassword().orNull();
        final Object privateKeyData = sshConfig.get(SshMachineLocation.PRIVATE_KEY_DATA.getName()) != null
                ? sshConfig.get(SshMachineLocation.PRIVATE_KEY_DATA.getName())
                : userCredentials.getOptionalPrivateKey().orNull();
        if (isManaged()) {
            final LocationSpec<JcloudsSshMachineLocation> spec = LocationSpec.create(JcloudsSshMachineLocation.class)
                    .configure(sshConfig)
                    .configure("displayName", displayName)
                    .configure("address", address)
                    .configure(JcloudsSshMachineLocation.SSH_PORT, port)
                    .configure("user", userCredentials.getUser())
                    // The use of `getName` is intentional. See 11741d85b9f54 for details.
                    .configure(SshMachineLocation.PASSWORD.getName(), password)
                    .configure(SshMachineLocation.PRIVATE_KEY_DATA.getName(), privateKeyData)
                    .configure("jcloudsParent", this)
                    .configure("node", node)
                    .configure("template", template.orNull())
                    .configureIfNotNull(CLOUD_AVAILABILITY_ZONE_ID, nodeAvailabilityZone)
                    .configureIfNotNull(CLOUD_REGION_ID, nodeRegion)
                    .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT))
                    .configure(SshMachineLocation.DETECT_MACHINE_DETAILS, setup.get(SshMachineLocation.DETECT_MACHINE_DETAILS))
                    .configureIfNotNull(SshMachineLocation.SCRIPT_DIR, setup.get(SshMachineLocation.SCRIPT_DIR))
                    .configureIfNotNull(USE_PORT_FORWARDING, setup.get(USE_PORT_FORWARDING))
                    .configureIfNotNull(PORT_FORWARDER, setup.get(PORT_FORWARDER))
                    .configureIfNotNull(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER))
                    .configureIfNotNull(SshMachineLocation.PRIVATE_ADDRESSES, node.getPrivateAddresses())
                    .configureIfNotNull(JCLOUDS_LOCATION_CUSTOMIZERS, customizers.size() > 0 ? customizers : null)
                    .configureIfNotNull(MACHINE_LOCATION_CUSTOMIZERS, machineCustomizers.size() > 0 ? machineCustomizers : null);
            return getManagementContext().getLocationManager().createLocation(spec);
        } else {
            LOG.warn("Using deprecated JcloudsSshMachineLocation constructor because " + this + " is not managed");
            final MutableMap.Builder<Object, Object> builder = MutableMap.builder()
                .putAll(sshConfig)
                .put("displayName", displayName)
                .put("address", address)
                .put("port", port)
                .put("user", userCredentials.getUser())
                // The use of `getName` is intentional. See 11741d85b9f54 for details.
                .putIfNotNull(SshMachineLocation.PASSWORD.getName(), password)
                .putIfNotNull(SshMachineLocation.PRIVATE_KEY_DATA.getName(), privateKeyData)
                .put("callerContext", setup.get(CALLER_CONTEXT))
                .putIfNotNull(CLOUD_AVAILABILITY_ZONE_ID.getName(), nodeAvailabilityZone)
                .putIfNotNull(CLOUD_REGION_ID.getName(), nodeRegion)
                .put(USE_PORT_FORWARDING, setup.get(USE_PORT_FORWARDING))
                .put(PORT_FORWARDER, setup.get(PORT_FORWARDER))
                .put(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER))
                .put(SshMachineLocation.PRIVATE_ADDRESSES, node.getPrivateAddresses());
            if (customizers.size() > 0) {
                builder.put(JCLOUDS_LOCATION_CUSTOMIZERS, customizers);
            }
            if (machineCustomizers.size() > 0) {
                builder.put(MACHINE_LOCATION_CUSTOMIZERS, machineCustomizers);
            }
            final MutableMap<Object, Object> properties = builder.build();
            return new JcloudsSshMachineLocation(properties, this, node);
        }
    }

    protected JcloudsWinRmMachineLocation registerWinRmMachineLocation(
            ComputeService computeService, NodeMetadata node, Optional<Template> template,
            LoginCredentials credentials, HostAndPort managementHostAndPort, ConfigBag setup) {
        JcloudsWinRmMachineLocation machine = createWinRmMachineLocation(computeService, node, template, credentials, managementHostAndPort, setup);
        registerJcloudsMachineLocation(node.getId(), machine);
        return machine;
    }

    protected JcloudsWinRmMachineLocation createWinRmMachineLocation(
            ComputeService computeService, NodeMetadata node, Optional<Template> template,
            LoginCredentials userCredentials, HostAndPort winrmHostAndPort, ConfigBag setup) {

        Collection<JcloudsLocationCustomizer> customizers = getCustomizers(setup);
        Collection<MachineLocationCustomizer> machineCustomizers = getMachineCustomizers(setup);
        Map<?,?> winrmConfig = extractWinrmConfig(setup, node);
        String nodeAvailabilityZone = extractAvailabilityZone(setup, node);
        String nodeRegion = extractRegion(setup, node);
        if (nodeRegion == null) {
            // e.g. rackspace doesn't have "region", so rackspace-uk is best we can say (but zone="LON")
            nodeRegion = extractProvider(setup, node);
        }
        
        String address = winrmHostAndPort.getHostText();
        String displayName = getPublicHostnameGeneric(node, setup, Optional.of(address));

        final Object password = winrmConfig.get(WinRmMachineLocation.PASSWORD.getName()) != null
                ? winrmConfig.get(WinRmMachineLocation.PASSWORD.getName())
                : userCredentials.getOptionalPassword().orNull();
        if (isManaged()) {
            final LocationSpec<JcloudsWinRmMachineLocation> spec = LocationSpec.create(JcloudsWinRmMachineLocation.class)
                    .configure(winrmConfig)
                    .configure("jcloudsParent", this)
                    .configure("displayName", displayName)
                    .configure("address", address)
                    .configure(WinRmMachineLocation.WINRM_CONFIG_PORT, winrmHostAndPort.getPort())
                    .configure(WinRmMachineLocation.USER.getName(), userCredentials.getUser())
                    .configure(WinRmMachineLocation.PASSWORD.getName(), password)
                    .configure("node", node)
                    .configureIfNotNull(CLOUD_AVAILABILITY_ZONE_ID, nodeAvailabilityZone)
                    .configureIfNotNull(CLOUD_REGION_ID, nodeRegion)
                    .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT))
                    .configure(SshMachineLocation.DETECT_MACHINE_DETAILS, setup.get(SshMachineLocation.DETECT_MACHINE_DETAILS))
                    .configureIfNotNull(SshMachineLocation.SCRIPT_DIR, setup.get(SshMachineLocation.SCRIPT_DIR))
                    .configureIfNotNull(USE_PORT_FORWARDING, setup.get(USE_PORT_FORWARDING))
                    .configureIfNotNull(PORT_FORWARDER, setup.get(PORT_FORWARDER))
                    .configureIfNotNull(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER))
                    .configureIfNotNull(JCLOUDS_LOCATION_CUSTOMIZERS, customizers.size() > 0 ? customizers : null)
                    .configureIfNotNull(MACHINE_LOCATION_CUSTOMIZERS, machineCustomizers.size() > 0 ? machineCustomizers : null);
            return getManagementContext().getLocationManager().createLocation(spec);
        } else {
            throw new UnsupportedOperationException("Cannot create WinRmMachineLocation because " + this + " is not managed");
        }
    }

    protected Map<String,Object> extractSshConfig(ConfigBag setup, NodeMetadata node) {
        ConfigBag nodeConfig = new ConfigBag();
        if (node!=null && node.getCredentials() != null) {
            nodeConfig.putIfNotNull(PASSWORD, node.getCredentials().getOptionalPassword().orNull());
            nodeConfig.putIfNotNull(PRIVATE_KEY_DATA, node.getCredentials().getOptionalPrivateKey().orNull());
        }
        return extractSshConfig(setup, nodeConfig).getAllConfig();
    }

    protected Map<String,Object> extractWinrmConfig(ConfigBag setup, NodeMetadata node) {
        ConfigBag nodeConfig = new ConfigBag();
        if (node!=null && node.getCredentials() != null) {
            nodeConfig.putIfNotNull(PASSWORD, node.getCredentials().getOptionalPassword().orNull());
            nodeConfig.putIfNotNull(PRIVATE_KEY_DATA, node.getCredentials().getOptionalPrivateKey().orNull());
        }
        return extractWinrmConfig(setup, nodeConfig).getAllConfig();
    }

    protected ConfigBag extractWinrmConfig(ConfigBag setup, ConfigBag alt) {
        ConfigBag winrmConfig = new ConfigBag();
        
        for (HasConfigKey<?> key : WinRmMachineLocation.ALL_WINRM_CONFIG_KEYS) {
            String keyName = key.getConfigKey().getName();
            if (setup.containsKey(keyName)) {
                winrmConfig.putStringKey(keyName, setup.getStringKey(keyName));
            } else if (alt.containsKey(keyName)) {
                winrmConfig.putStringKey(keyName, setup.getStringKey(keyName));
            }
        }
        
        Map<String, Object> winrmToolClassProperties = Maps.filterKeys(setup.getAllConfig(), StringPredicates.startsWith(WinRmMachineLocation.WINRM_TOOL_CLASS_PROPERTIES_PREFIX));
        winrmConfig.putAll(winrmToolClassProperties);
        
        return winrmConfig;
    }

    protected String extractAvailabilityZone(ConfigBag setup, NodeMetadata node) {
        return extractNodeLocationId(setup, node, LocationScope.ZONE);
    }

    protected String extractRegion(ConfigBag setup, NodeMetadata node) {
        return extractNodeLocationId(setup, node, LocationScope.REGION);
    }

    protected String extractProvider(ConfigBag setup, NodeMetadata node) {
        return extractNodeLocationId(setup, node, LocationScope.PROVIDER);
    }

    protected String extractNodeLocationId(ConfigBag setup, NodeMetadata node, LocationScope scope) {
        org.jclouds.domain.Location nodeLoc = node.getLocation();
        if(nodeLoc == null) return null;
        do {
            if (nodeLoc.getScope() == scope) return nodeLoc.getId();
            nodeLoc = nodeLoc.getParent();
        } while (nodeLoc != null);
        return null;
    }

    // -------------- give back the machines------------------

    @Override
    public void release(MachineLocation rawMachine) {
        String instanceId = vmInstanceIds.remove(rawMachine);
        if (instanceId == null) {
            LOG.info("Attempted release of unknown machine "+rawMachine+" in "+toString());
            throw new IllegalArgumentException("Unknown machine "+rawMachine);
        }
        JcloudsMachineLocation machine = (JcloudsMachineLocation) rawMachine;

        LOG.info("Releasing machine {} in {}, instance id {}", new Object[] {machine, this, instanceId});

        Exception tothrow = null;

        ConfigBag setup = ((LocationInternal)machine).config().getBag();
        Collection<JcloudsLocationCustomizer> customizers = getCustomizers(setup);
        Collection<MachineLocationCustomizer> machineCustomizers = getMachineCustomizers(setup);
        
        for (JcloudsLocationCustomizer customizer : customizers) {
            try {
                customizer.preRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release customizer "+customizer+" for machine "+machine+" in "+this+", instance id "+instanceId+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        for (MachineLocationCustomizer customizer : machineCustomizers) {
            customizer.preRelease(machine);
        }

        try {
            // FIXME: Needs to release port forwarding for WinRmMachineLocations
            if (machine instanceof JcloudsMachineLocation) {
                releasePortForwarding(machine);
            }
        } catch (Exception e) {
            LOG.error("Problem releasing port-forwarding for machine "+machine+" in "+this+", instance id "+instanceId+
                "; ignoring and continuing, "
                + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
            if (tothrow==null) tothrow = e;
        }

        try {
            releaseNode(instanceId);
        } catch (Exception e) {
            LOG.error("Problem releasing machine "+machine+" in "+this+", instance id "+instanceId+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
            if (tothrow==null) tothrow = e;
        }

        removeChild(machine);

        for (JcloudsLocationCustomizer customizer : customizers) {
            try {
                customizer.postRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release customizer "+customizer+" for machine "+machine+" in "+this+", instance id "+instanceId+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        
        if (tothrow != null) {
            throw Exceptions.propagate(tothrow);
        }
    }

    protected void releaseSafely(MachineLocation machine) {
        try {
            release(machine);
        } catch (Exception e) {
            // rely on exception having been logged by #release(SshMachineLocation), so no-op
        }
    }

    protected void releaseNodeSafely(NodeMetadata node) {
        String instanceId = node.getId();
        LOG.info("Releasing node {} in {}, instance id {}", new Object[] {node, this, instanceId});

        try {
            releaseNode(instanceId);
        } catch (Exception e) {
            LOG.warn("Problem releasing node "+node+" in "+this+", instance id "+instanceId+
                    "; discarding instance and continuing...", e);
        }
    }

    protected void releaseNode(String instanceId) {
        ComputeService computeService = null;
        try {
            computeService = getComputeService(config().getBag());
            computeService.destroyNode(instanceId);
        } finally {
        /*
            // we don't close the compute service; this means if we provision add'l it is fast;
            // however it also means an explicit System.exit may be needed for termination
            if (computeService != null) {
                try {
                    computeService.getContext().close();
                } catch (Exception e) {
                    LOG.error "Problem closing compute-service's context; continuing...", e
                }
            }
         */
        }
    }

    protected void releasePortForwarding(final JcloudsMachineLocation machine) {
        // TODO Implementation needs revisisted. It relies on deprecated PortForwardManager methods.

        boolean usePortForwarding = Boolean.TRUE.equals(machine.getConfig(USE_PORT_FORWARDING));
        final JcloudsPortForwarderExtension portForwarder = machine.getConfig(PORT_FORWARDER);
        final String nodeId = machine.getJcloudsId();
        final Map<String, Runnable> subtasks = Maps.newLinkedHashMap();

        PortForwardManager portForwardManager = machine.getConfig(PORT_FORWARDING_MANAGER);
        if (portForwardManager == null) {
            LOG.debug("No PortForwardManager, using default");
            portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        }

        if (portForwarder == null) {
            LOG.debug("No port-forwarding to close (because portForwarder null) on release of " + machine);
        } else {
            final Optional<NodeMetadata> node = machine.getOptionalNode();
            // Release the port-forwarding for the login-port, which was explicitly created by JcloudsLocation
            if (usePortForwarding && node.isPresent()) {
                final HostAndPort hostAndPortOverride;
                if (machine instanceof SshMachineLocation) {
                    hostAndPortOverride = ((SshMachineLocation)machine).getSshHostAndPort();
                } else if (machine instanceof WinRmMachineLocation) {
                    String host = ((WinRmMachineLocation)machine).getAddress().getHostAddress();
                    int port = ((WinRmMachineLocation)machine).getPort();
                    hostAndPortOverride = HostAndPort.fromParts(host, port);
                } else {
                    LOG.warn("Unexpected machine {} of type {}; expected SSH or WinRM", machine, (machine != null ? machine.getClass() : null));
                    hostAndPortOverride = null;
                }
                if (hostAndPortOverride != null) {
                    final int loginPort = node.get().getLoginPort();
                    subtasks.put(
                            "Close port-forward "+hostAndPortOverride+"->"+loginPort,
                            new Runnable() {
                                @Override
                                public void run() {
                                    LOG.debug("Closing port-forwarding at {} for machine {}: {}->{}", new Object[] {this, machine, hostAndPortOverride, loginPort});
                                    portForwarder.closePortForwarding(node.get(), loginPort, hostAndPortOverride, Protocol.TCP);
                                }
                            });
                }
            }

            // Get all the other port-forwarding mappings for this VM, and release all of those
            Set<PortMapping> mappings = Sets.newLinkedHashSet();
            mappings.addAll(portForwardManager.getLocationPublicIpIds(machine));
            if (nodeId != null) {
                mappings.addAll(portForwardManager.getPortMappingWithPublicIpId(nodeId));
            }

            for (final PortMapping mapping : mappings) {
                final HostAndPort publicEndpoint = mapping.getPublicEndpoint();
                final int targetPort = mapping.getPrivatePort();
                final Protocol protocol = Protocol.TCP;
                if (publicEndpoint != null && node.isPresent()) {
                    subtasks.put(
                            "Close port-forward "+publicEndpoint+"->"+targetPort,
                            new Runnable() {
                                @Override
                                public void run() {
                                    LOG.debug("Closing port-forwarding at {} for machine {}: {}->{}", new Object[] {this, machine, publicEndpoint, targetPort});
                                    portForwarder.closePortForwarding(node.get(), targetPort, publicEndpoint, protocol);
                                }
                            });
                }
            }

            if (subtasks.size() > 0) {
                final TaskBuilder<Void> builder = TaskBuilder.<Void>builder()
                        .parallel(true)
                        .displayName("close port-forwarding at "+machine);
                for (Map.Entry<String, Runnable> entry : subtasks.entrySet()) {
                    builder.add(TaskBuilder.builder().displayName(entry.getKey()).body(entry.getValue()).build());
                }
                final Task<Void> task = builder.build();
                final DynamicTasks.TaskQueueingResult<Void> queueResult = DynamicTasks.queueIfPossible(task);
                if(queueResult.isQueuedOrSubmitted()){
                    final String origDetails = Tasks.setBlockingDetails("waiting for closing port-forwarding of "+machine);
                    try {
                        task.blockUntilEnded();
                    } finally {
                        Tasks.setBlockingDetails(origDetails);
                    }
                } else {
                    LOG.warn("Releasing port-forwarding of "+machine+" not executing in execution-context "
                            + "(e.g. not invoked inside effector); falling back to executing sequentially");
                    for (Runnable subtask : subtasks.values()) {
                        subtask.run();
                    }
                }
            }
        }

        // Forget all port mappings associated with this VM
        portForwardManager.forgetPortMappings(machine);
        if (nodeId != null) {
            portForwardManager.forgetPortMappings(nodeId);
        }
    }

    // ------------ support methods --------------------

    /**
     * Extracts the user that jclouds tells us about (i.e. from the jclouds node).
     * <p>
     * Modifies <code>setup</code> to set {@link #USER} if it is unset when the method is called or
     * if the value in the bag is {@link #ROOT_USERNAME} and the user on the node is contained in
     * {@link #ROOT_ALIASES}.
     */
    protected LoginCredentials extractVmCredentials(ConfigBag setup, NodeMetadata node, LoginCredentials nodeCredentials) {
        boolean windows = isWindows(node, setup);
        String user = getUser(setup);
        OsCredential localCredentials = LocationConfigUtils.getOsCredential(setup).checkNoErrors();
        
        LOG.debug("Credentials extracted for {}: {}/{} with {}/{}", new Object[] {
                node, user, nodeCredentials.getUser(), localCredentials, nodeCredentials });

        if (Strings.isNonBlank(nodeCredentials.getUser())) {
            if (Strings.isBlank(user)) {
                setup.put(USER, user = nodeCredentials.getUser());
            } else if (ROOT_USERNAME.equals(user) && ROOT_ALIASES.contains(nodeCredentials.getUser())) {
                // deprecated, we used to default username to 'root'; now we leave null, then use autodetected credentials if no user specified
                LOG.warn("overriding username 'root' in favour of '"+nodeCredentials.getUser()+"' at {}; this behaviour may be removed in future", node);
                setup.put(USER, user = nodeCredentials.getUser());
            }

            String pkd = Strings.maybeNonBlank(localCredentials.getPrivateKeyData())
                    .or(nodeCredentials.getOptionalPrivateKey().orNull());
            String pwd = Strings.maybeNonBlank(localCredentials.getPassword())
                    .or(nodeCredentials.getOptionalPassword().orNull());
            if (Strings.isBlank(user) || (Strings.isBlank(pkd) && pwd==null)) {
                String missing = (user==null ? "user" : "credential");
                LOG.warn("Not able to determine "+missing+" for "+this+" at "+node+"; will likely fail subsequently");
                return null;
            } else {
                LoginCredentials.Builder resultBuilder = LoginCredentials.builder().user(user);
                if (pwd != null && (Strings.isBlank(pkd) || localCredentials.isUsingPassword() || windows)) {
                    resultBuilder.password(pwd);
                } else { // pkd guaranteed non-blank due to above
                    resultBuilder.privateKey(pkd);
                }
                return resultBuilder.build();
            }
        }

        LOG.warn("No node-credentials or admin-access available for node "+node+" in "+this+"; will likely fail subsequently");
        return null;
    }

    protected String getFirstReachableAddress(NodeMetadata node, ConfigBag setup) {
        String pollForFirstReachable = setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS);

        boolean enabled = !"false".equalsIgnoreCase(pollForFirstReachable);
        String result;
        if (enabled) {
            Duration timeout = "true".equals(pollForFirstReachable) ? Duration.FIVE_MINUTES : Duration.of(pollForFirstReachable);
            Predicate<? super HostAndPort> predicate = getReachableAddressesPredicate(setup);
            LOG.debug("{} polling for first reachable address with {}", this, predicate);
            // Throws if no suitable address is found.
            result = JcloudsUtil.getFirstReachableAddress(node, timeout, predicate);
            LOG.debug("Using first-reachable address "+result+" for node "+node+" in "+this);
        } else {
            result = Iterables.getFirst(Iterables.concat(node.getPublicAddresses(), node.getPrivateAddresses()), null);
            if (result == null) {
                throw new IllegalStateException("No addresses available for node "+node+" in "+this);
            }
            LOG.debug("Using first address "+result+" for node "+node+" in "+this);
        }
        return result;
    }

    private Predicate<? super HostAndPort> getReachableAddressesPredicate(ConfigBag config) {
        Predicate<? super HostAndPort> pollForFirstReachableHostAndPortPredicate;
        if (config.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE) != null) {
            return config.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE);
        } else {
            Class<? extends Predicate<? super HostAndPort>> predicateType =
                    config.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE);

            Map<String, Object> args = MutableMap.of();
            ConfigUtils.addUnprefixedConfigKeyInConfigBack(
                    POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ".", config, args);
            try {
                return predicateType.getConstructor(Map.class).newInstance(args);
            } catch (NoSuchMethodException | IllegalAccessException e) {
                try {
                    return pollForFirstReachableHostAndPortPredicate = predicateType.newInstance();
                } catch (IllegalAccessException | InstantiationException newInstanceException) {
                    throw Exceptions.propagate("Failed to instantiate " + predicateType, newInstanceException);
                }
            } catch (InvocationTargetException | InstantiationException e) {
                throw Exceptions.propagate("Failed to instantiate " + predicateType + " with Map constructor", e);
            }
        }
    }

    protected LoginCredentials waitForWinRmAvailable(LoginCredentials credentialsToTry, final HostAndPort managementHostAndPort, ConfigBag setup) {
        String waitForWinrmAvailable = setup.get(WAIT_FOR_WINRM_AVAILABLE);
        checkArgument(!"false".equalsIgnoreCase(waitForWinrmAvailable), "waitForWinRmAvailable called despite waitForWinRmAvailable=%s", waitForWinrmAvailable);
        Duration timeout = null;
        try {
            timeout = Duration.parse(waitForWinrmAvailable);
        } catch (Exception e) {
            // TODO will this just be a NumberFormatException? If so, catch that specificially
            // normal if 'true'; just fall back to default
            Exceptions.propagateIfFatal(e);
        }
        if (timeout == null) {
            timeout = Duration.parse(WAIT_FOR_WINRM_AVAILABLE.getDefaultValue());
        }

        String user = credentialsToTry.getUser();

        String connectionDetails = user + "@" + managementHostAndPort;
        final AtomicReference<LoginCredentials> credsSuccessful = new AtomicReference<LoginCredentials>();

        // Don't use config that relates to the final user credentials (those have nothing to do
        // with the initial credentials of the VM returned by the cloud provider).
        // The createTemporaryWinRmMachineLocation deals with removing that.
        ConfigBag winrmProps = ConfigBag.newInstanceCopying(setup);

        final Pair<WinRmMachineLocation, LoginCredentials> machinesToTry = Pair.of(
                createTemporaryWinRmMachineLocation(managementHostAndPort, credentialsToTry, winrmProps), credentialsToTry);

        try {
            Callable<Boolean> checker = new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    final WinRmMachineLocation machine = machinesToTry.getLeft();
                    WinRmToolResponse response = machine.executeCommand(
                            ImmutableMap.of(WinRmTool.PROP_EXEC_TRIES.getName(), 1),
                            ImmutableList.of("echo testing"));
                    boolean success = (response.getStatusCode() == 0);
                    if (success) {
                        credsSuccessful.set(machinesToTry.getRight());

                        String verifyWindowsUp = getConfig(WinRmMachineLocation.WAIT_WINDOWS_TO_START);
                        if (Strings.isBlank(verifyWindowsUp) || verifyWindowsUp.equals("false")) {
                            return true;
                        }

                        Predicate<WinRmMachineLocation> machineReachable = new Predicate<WinRmMachineLocation>() {
                            @Override
                            public boolean apply(@Nullable WinRmMachineLocation machine) {
                                try {
                                    WinRmToolResponse response = machine.executeCommand("echo testing");
                                    int statusCode = response.getStatusCode();
                                    return statusCode == 0;
                                } catch (RuntimeException e) {
                                    if (getFirstThrowableOfType(e, IOException.class) != null || getFirstThrowableOfType(e, WebServiceException.class) != null) {
                                        LOG.debug("WinRM Connectivity lost", e);
                                        return false;
                                    } else {
                                        throw e;
                                    }
                                }
                            }
                        };
                        Duration verifyWindowsUpTime = Duration.of(verifyWindowsUp);
                        boolean restartHappened = Predicates2.retry(Predicates.not(machineReachable),
                                verifyWindowsUpTime.toMilliseconds(),
                                Duration.FIVE_SECONDS.toMilliseconds(),
                                Duration.THIRTY_SECONDS.toMilliseconds(),
                                TimeUnit.MILLISECONDS).apply(machine);
                        if (restartHappened) {
                            LOG.info("Connectivity to the machine was lost. Probably Windows have restarted {} as part of the provisioning process.\nRetrying to connect...", machine);
                            return Predicates2.retry(machineReachable,
                                    verifyWindowsUpTime.toMilliseconds(),
                                    Duration.of(5, TimeUnit.SECONDS).toMilliseconds(),
                                    Duration.of(30, TimeUnit.SECONDS).toMilliseconds(),
                                    TimeUnit.MILLISECONDS).apply(machine);
                        } else {
                            return true;
                        }
                    }
                    return false;
                }};

            waitForReachable(checker, connectionDetails, ImmutableList.of(credentialsToTry), setup, timeout);
        } finally {
            if (getManagementContext().getLocationManager().isManaged(machinesToTry.getLeft())) {
                // get benign but unpleasant warnings if we unmanage something already unmanaged
                getManagementContext().getLocationManager().unmanage(machinesToTry.getLeft());
            }
        }
        return credsSuccessful.get();
    }

    protected LoginCredentials waitForSshableGuessCredentials(final ComputeService computeService, final NodeMetadata node, HostAndPort managementHostAndPort, ConfigBag setup) {
        // See https://issues.apache.org/jira/browse/BROOKLYN-186
        // Handle where jclouds gives us the wrong login user (!) and both a password + ssh key.
        // Try all the permutations to find the one that works.
        Iterable<LoginCredentials> credentialsToTry = generateCredentials(node.getCredentials(), setup.get(LOGIN_USER));
        return waitForSshable(computeService, node, managementHostAndPort, credentialsToTry, setup);
    }

    /** @deprecated Since 0.11.0. Use {@link #waitForSshableGuessCredentials} instead. */
    @Deprecated
    protected LoginCredentials waitForSshable(ComputeService computeService, NodeMetadata node, HostAndPort managementHostAndPort, ConfigBag setup) {
        return waitForSshableGuessCredentials(computeService, node, managementHostAndPort, setup);
    }

    /** @return An Iterable of credentials based on nodeCreds containing different parameters. */
    Iterable<LoginCredentials> generateCredentials(LoginCredentials nodeCreds, @Nullable String loginUserOverride) {
        String nodeUser = nodeCreds.getUser();
        Set<String> users = MutableSet.of();
        if (Strings.isNonBlank(nodeUser)) {
            users.add(nodeUser);
        }
        if (Strings.isNonBlank(loginUserOverride)) {
            users.add(loginUserOverride);
        }
        List<LoginCredentials> credentialsToTry = new ArrayList<>();
        for (String user : users) {
            if (nodeCreds.getOptionalPassword().isPresent() && nodeCreds.getOptionalPrivateKey().isPresent()) {
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).noPassword().user(user).build());
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).noPrivateKey().user(user).build());
            } else {
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).user(user).build());
            }
        }
        return credentialsToTry;
    }

    /** @deprecated since 0.11.0 use {@link #waitForSshable(HostAndPort, Iterable, ConfigBag)} instead */
    @Deprecated
    protected LoginCredentials waitForSshable(
            final ComputeService computeService, final NodeMetadata node, HostAndPort hostAndPort,
            Iterable<LoginCredentials> credentialsToTry, ConfigBag setup) {
        return waitForSshable(hostAndPort, credentialsToTry, setup);
    }

    protected LoginCredentials waitForSshable(
            HostAndPort hostAndPort, Iterable<LoginCredentials> credentialsToTry, ConfigBag setup) {
        String waitForSshable = setup.get(WAIT_FOR_SSHABLE);
        checkArgument(!"false".equalsIgnoreCase(waitForSshable), "waitForSshable called despite waitForSshable=%s for %s", waitForSshable, hostAndPort);
        checkArgument(!Iterables.isEmpty(credentialsToTry), "waitForSshable called without credentials for %s", hostAndPort);

        Duration timeout = null;
        try {
            timeout = Duration.parse(waitForSshable);
        } catch (Exception e) {
            // normal if 'true'; just fall back to default
        }
        if (timeout == null) {
            timeout = Duration.parse(WAIT_FOR_SSHABLE.getDefaultValue());
        }

        Set<String> users = Sets.newLinkedHashSet();
        for (LoginCredentials creds : credentialsToTry) {
            users.add(creds.getUser());
        }
        String user = (users.size() == 1) ? Iterables.getOnlyElement(users) : "{" + Joiner.on(",").join(users) + "}";

        String connectionDetails = user + "@" + hostAndPort;
        final AtomicReference<LoginCredentials> credsSuccessful = new AtomicReference<LoginCredentials>();

        // Don't use config that relates to the final user credentials (those have nothing to do
        // with the initial credentials of the VM returned by the cloud provider).
        ConfigBag sshProps = ConfigBag.newInstanceCopying(setup);
        sshProps.remove("password");
        sshProps.remove("privateKeyData");
        sshProps.remove("privateKeyFile");
        sshProps.remove("privateKeyPassphrase");

        final Map<SshMachineLocation, LoginCredentials> machinesToTry = Maps.newLinkedHashMap();
        for (LoginCredentials creds : credentialsToTry) {
            machinesToTry.put(createTemporarySshMachineLocation(hostAndPort, creds, sshProps), creds);
        }
        final Duration repeaterTimeout = timeout;
        try {
            Callable<Boolean> checker = new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    for (Map.Entry<SshMachineLocation, LoginCredentials> entry : machinesToTry.entrySet()) {
                        SshMachineLocation machine = entry.getKey();
                        Duration statusTimeout = Duration.THIRTY_SECONDS.isShorterThan(repeaterTimeout)
                                ? Duration.THIRTY_SECONDS
                                : repeaterTimeout;
                        int exitstatus = machine.execScript(
                                ImmutableMap.of(
                                        SshTool.PROP_CONNECT_TIMEOUT.getName(), statusTimeout.toMilliseconds(),
                                        SshTool.PROP_SESSION_TIMEOUT.getName(), statusTimeout.toMilliseconds(),
                                        SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), statusTimeout.toMilliseconds(),
                                        SshTool.PROP_SSH_TRIES.getName(), 1),
                                "check-connectivity",
                                ImmutableList.of("true"));
                        boolean success = (exitstatus == 0);
                        if (success) {
                            credsSuccessful.set(entry.getValue());
                            return true;
                        }
                    }
                    return false;
                }};

            waitForReachable(checker, connectionDetails, credentialsToTry, setup, timeout);
        } finally {
            for (SshMachineLocation machine : machinesToTry.keySet()) {
                if (getManagementContext().getLocationManager().isManaged(machine)) {
                    // get benign but unpleasant warnings if we unmanage something already unmanaged
                    getManagementContext().getLocationManager().unmanage(machine);
                }
                Streams.closeQuietly(machine);
            }
        }

        return credsSuccessful.get();
    }

    @VisibleForTesting
    static int getLoginPortOrDefault(NodeMetadata node, int defaultPort) {
        int loginPort = node.getLoginPort();
        if (loginPort > 0) {
            return loginPort;
        }
        return defaultPort;
    }

    protected void waitForReachable(Callable<Boolean> checker, String hostAndPort, Iterable<LoginCredentials> credentialsToLog, ConfigBag setup, Duration timeout) {
        if (LOG.isDebugEnabled()) {
            List<String> credsToString = Lists.newArrayList();
            for (LoginCredentials creds : credentialsToLog) {
                String user = creds.getUser();
                String password;
                String key;
                if (Boolean.TRUE.equals(setup.get(LOG_CREDENTIALS))) {
                    password = creds.getOptionalPassword().or("<absent>");
                    key = creds.getOptionalPrivateKey().or("<absent>");
                } else {
                    password = creds.getOptionalPassword().isPresent() ? "******" : "<absent>";
                    key = creds.getOptionalPrivateKey().isPresent() ? "******" : "<absent>";
                }
                credsToString.add("user="+user+", password="+password+", key="+key);
            }

            LOG.debug("VM {}: reported online, now waiting {} for it to be contactable on {}; trying {} credential{}: {}",
                    new Object[] {
                            getCreationString(setup), timeout,
                            hostAndPort,
                            Iterables.size(credentialsToLog),
                            Strings.s(Iterables.size(credentialsToLog)),
                            (credsToString.size() == 1) ? credsToString.get(0) : "(multiple!):" + Joiner.on("\n\t").join(credsToString)
                    });
        }

        Stopwatch stopwatch = Stopwatch.createStarted();

        ReferenceWithError<Boolean> reachable = new Repeater("reachable repeater ")
                .backoff(Duration.ONE_SECOND, 2, Duration.TEN_SECONDS) // exponential backoff, to 10 seconds
                .until(checker)
                .limitTimeTo(timeout)
                .runKeepingError();

        if (!reachable.getWithoutError()) {
            throw new IllegalStateException("Connection failed for "
                    +hostAndPort+" ("+getCreationString(setup)+") after waiting "
                    +Time.makeTimeStringRounded(timeout), reachable.getError());
        }

        LOG.debug("VM {}: connection succeeded after {} on {}",new Object[] {
                getCreationString(setup), Time.makeTimeStringRounded(stopwatch),
                hostAndPort});
    }


    // -------------------- hostnames ------------------------
    // hostnames are complicated, but irregardless, this code could be cleaned up!

    protected void setHostnameUpdatingCredentials(ConfigBag setup, NodeMetadata metadata) {
        List<String> usersTried = new ArrayList<String>();

        String originalUser = getUser(setup);
        if (groovyTruth(originalUser)) {
            if (setHostname(setup, metadata, false)) return;
            usersTried.add(originalUser);
        }

        LoginCredentials credentials = metadata.getCredentials();
        if (credentials!=null) {
            if (Strings.isNonBlank(credentials.getUser())) setup.put(USER, credentials.getUser());
            if (Strings.isNonBlank(credentials.getOptionalPrivateKey().orNull())) setup.put(PRIVATE_KEY_DATA, credentials.getOptionalPrivateKey().orNull());
            if (setHostname(setup, metadata, false)) {
                if (originalUser!=null && !originalUser.equals(getUser(setup))) {
                    LOG.warn("Switching to cloud-specified user at "+metadata+" as "+getUser(setup)+" (failed to connect using: "+usersTried+")");
                }
                return;
            }
            usersTried.add(getUser(setup));
        }

        for (String u: COMMON_USER_NAMES_TO_TRY) {
            setup.put(USER, u);
            if (setHostname(setup, metadata, false)) {
                LOG.warn("Auto-detected user at "+metadata+" as "+getUser(setup)+" (failed to connect using: "+usersTried+")");
                return;
            }
            usersTried.add(getUser(setup));
        }
        // just repeat, so we throw exception
        LOG.warn("Failed to log in to "+metadata+", tried as users "+usersTried+" (throwing original exception)");
        setup.put(USER, originalUser);
        setHostname(setup, metadata, true);
    }

    protected boolean setHostname(ConfigBag setup, NodeMetadata metadata, boolean rethrow) {
        try {
            setup.put(SshTool.PROP_HOST, getPublicHostname(metadata, Optional.<HostAndPort>absent(), setup));
            return true;
        } catch (Exception e) {
            if (rethrow) {
                LOG.warn("couldn't connect to "+metadata+" when trying to discover hostname (rethrowing): "+e);
                throw Exceptions.propagate(e);
            }
            return false;
        }
    }

    protected String getPublicHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) {
        return getPublicHostname(node, sshHostAndPort, node.getCredentials(), setup);
    }
    
    /**
     * Attempts to obtain the hostname or IP of the node, as advertised by the cloud provider.
     * Prefers public, reachable IPs.
     * For some clouds (e.g. aws-ec2), it will attempt to find the public hostname.
     */
    protected String getPublicHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, LoginCredentials userCredentials, ConfigBag setup) {
        return getPublicHostname(node, sshHostAndPort, Suppliers.ofInstance(userCredentials), setup);
    }
    
    protected String getPublicHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, Supplier<? extends LoginCredentials> userCredentials, ConfigBag setup) {
        String provider = (setup != null) ? setup.get(CLOUD_PROVIDER) : null;
        Boolean lookupAwsHostname = (setup != null) ? setup.get(LOOKUP_AWS_HOSTNAME) : null;
        if (provider == null) provider= getProvider();

        if ("aws-ec2".equals(provider) && Boolean.TRUE.equals(lookupAwsHostname)) {
            Maybe<String> result = getHostnameAws(node, sshHostAndPort, userCredentials, setup);
            if (result.isPresent()) return result.get();
        }

        Optional<String> preferredAddress = sshHostAndPort.isPresent() ? Optional.of(sshHostAndPort.get().getHostText()) : Optional.<String>absent();
        return getPublicHostnameGeneric(node, setup, preferredAddress);
    }

    /**
     * Attempts to obtain the private hostname or IP of the node, as advertised by the cloud provider.
     * 
     * For some clouds (e.g. aws-ec2), it will attempt to find the fully qualified hostname (as that works in public+private).
     */
    protected String getPrivateHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) {
        return getPrivateHostname(node, sshHostAndPort, node.getCredentials(), setup);
    }

    protected String getPrivateHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, LoginCredentials userCredentials, ConfigBag setup) {
        return getPrivateHostname(node, sshHostAndPort, Suppliers.ofInstance(userCredentials), setup);
    }
    
    protected String getPrivateHostname(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, Supplier<? extends LoginCredentials> userCredentials, ConfigBag setup) {
        Boolean useMachinePublicAddressAsPrivateAddress = (setup != null) ? setup.get(USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS) : false;
        if(useMachinePublicAddressAsPrivateAddress) {
            LOG.debug("Overriding private hostname as public hostname because config "+ USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS.getName()+" is set to true");
            return getPublicHostname(node, sshHostAndPort, userCredentials, setup);
        }

        String provider = (setup != null) ? setup.get(CLOUD_PROVIDER) : null;
        Boolean lookupAwsHostname = (setup != null) ? setup.get(LOOKUP_AWS_HOSTNAME) : null;
        if (provider == null) provider = getProvider();

        // TODO Discouraged to do cloud-specific things; think of this code for aws as an
        // exceptional situation rather than a pattern to follow. We need a better way to
        // do cloud-specific things.
        if ("aws-ec2".equals(provider) && Boolean.TRUE.equals(lookupAwsHostname)) {
            Maybe<String> result = getHostnameAws(node, sshHostAndPort, userCredentials, setup);
            if (result.isPresent()) return result.get();
        }

        Optional<String> preferredAddress = sshHostAndPort.isPresent() ? Optional.of(sshHostAndPort.get().getHostText()) : Optional.<String>absent();
        return getPrivateHostnameGeneric(node, setup, preferredAddress);
    }

    private String getPublicHostnameGeneric(NodeMetadata node, @Nullable ConfigBag setup) {
        return getPublicHostnameGeneric(node, setup, Optional.<String>absent());
    }
    
    /**
     * The preferredAddress is returned if it is one of the best choices (e.g. if publicAddresses 
     * contains it, or if publicAddresses.isEmpty but the privateAddresses contains it).
     * 
     * Otherwise, returns the first publicAddress (if any), or failing that the first privateAddress.
     */
    private String getPublicHostnameGeneric(NodeMetadata node, @Nullable ConfigBag setup, Optional<String> preferredAddress) {
        // JcloudsUtil.getFirstReachableAddress() (probably) already succeeded so at least one of the provided
        // public and private IPs is reachable. Prefer the public IP. Don't use hostname as a fallback
        // from the public address - if public address is missing why would hostname resolve to a 
        // public IP? It is sometimes wrong/abbreviated, resolving to the wrong IP, also e.g. on
        // rackspace, the hostname lacks the domain.
        //
        // TODO If POLL_FOR_FIRST_REACHABLE_ADDRESS=false, then won't have checked if any node is reachable.
        // TODO Some of the private addresses might not be reachable, should check connectivity before
        // making a choice.
        // TODO Choose an IP once and stick to it - multiple places call JcloudsUtil.getFirstReachableAddress(),
        // could even get different IP on each call.
        if (groovyTruth(node.getPublicAddresses())) {
            if (preferredAddress.isPresent() && node.getPublicAddresses().contains(preferredAddress.get())) {
                return preferredAddress.get();
            }
            return node.getPublicAddresses().iterator().next();
        } else if (groovyTruth(node.getPrivateAddresses())) {
            if (preferredAddress.isPresent() && node.getPrivateAddresses().contains(preferredAddress.get())) {
                return preferredAddress.get();
            }
            return node.getPrivateAddresses().iterator().next();
        } else {
            return null;
        }
    }

    /**
     * The preferredAddress is returned if it is one of the best choices (e.g. if non-local privateAddresses 
     * contains it, or if privateAddresses.isEmpty but the publicAddresses contains it).
     * 
     * Otherwise, returns the first publicAddress (if any), or failing that the first privateAddress.
     */
    private String getPrivateHostnameGeneric(NodeMetadata node, @Nullable ConfigBag setup, Optional<String> preferredAddress) {
        //prefer the private address to the hostname because hostname is sometimes wrong/abbreviated
        //(see that javadoc; also e.g. on rackspace/cloudstack, the hostname is not registered with any DNS).
        //Don't return local-only address (e.g. never 127.0.0.1)
        Iterable<String> privateAddresses = Iterables.filter(node.getPrivateAddresses(), new Predicate<String>() {
            @Override public boolean apply(String input) {
                return input != null && !Networking.isLocalOnly(input);
            }});
        if (!Iterables.isEmpty(privateAddresses)) {
            if (preferredAddress.isPresent() && Iterables.contains(privateAddresses, preferredAddress.get())) {
                return preferredAddress.get();
            }
            return Iterables.get(privateAddresses, 0);
        }
        
        if (groovyTruth(node.getPublicAddresses())) {
            if (preferredAddress.isPresent() && node.getPublicAddresses().contains(preferredAddress.get())) {
                return preferredAddress.get();
            }
            return node.getPublicAddresses().iterator().next();
        } else if (groovyTruth(node.getHostname())) {
            return node.getHostname();
        } else {
            return null;
        }
    }

    Maybe<String> getHostnameAws(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, Supplier<? extends LoginCredentials> userCredentials, ConfigBag setup) {
        HostAndPort inferredHostAndPort = null;
        boolean waitForSshable = !"false".equalsIgnoreCase(setup.get(WAIT_FOR_SSHABLE));
        if (!waitForSshable) {
            return Maybe.absent();
        }

        if (!sshHostAndPort.isPresent()) {
            try {
                String vmIp = getFirstReachableAddress(node, setup);
                int port = node.getLoginPort();
                inferredHostAndPort = HostAndPort.fromParts(vmIp, port);
            } catch (Exception e) {
                LOG.warn("Error reaching aws-ec2 instance "+node.getId()+"@"+node.getLocation()+" on port "+node.getLoginPort()+"; falling back to jclouds metadata for address", e);
            }
        }
        if (sshHostAndPort.isPresent() || inferredHostAndPort != null) {
            if (isWindows(node, setup)) {
                LOG.warn("Cannot query aws-ec2 Windows instance "+node.getId()+"@"+node.getLocation()+" over ssh for its hostname; falling back to jclouds metadata for address");
            } else {
                HostAndPort hostAndPortToUse = sshHostAndPort.isPresent() ? sshHostAndPort.get() : inferredHostAndPort;
                try {
                    return Maybe.of(getHostnameAws(hostAndPortToUse, userCredentials.get(), setup));
                } catch (Exception e) {
                    LOG.warn("Error querying aws-ec2 instance "+node.getId()+"@"+node.getLocation()+" over ssh for its hostname; falling back to jclouds metadata for address", e);
                }
            }
        }
        return Maybe.absent();
    }

    String getHostnameAws(HostAndPort hostAndPort, LoginCredentials userCredentials, ConfigBag setup) {
        SshMachineLocation sshLocByIp = null;
        try {
            // TODO messy way to get an SSH session
            sshLocByIp = createTemporarySshMachineLocation(hostAndPort, userCredentials, setup);

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ByteArrayOutputStream errStream = new ByteArrayOutputStream();
            int exitcode = sshLocByIp.execCommands(
                    MutableMap.of("out", outStream, "err", errStream),
                    "get public AWS hostname",
                    ImmutableList.of(
                            BashCommands.INSTALL_CURL,
                            "echo `curl --silent --retry 20 http://169.254.169.254/latest/meta-data/public-hostname`; exit"));
            String outString = new String(outStream.toByteArray());
            String[] outLines = outString.split("\n");
            for (String line : outLines) {
                if (line.startsWith("ec2-")) return line.trim();
            }
            throw new IllegalStateException("Could not obtain aws-ec2 hostname for vm "+hostAndPort+"; exitcode="+exitcode+"; stdout="+outString+"; stderr="+new String(errStream.toByteArray()));
        } finally {
            Streams.closeQuietly(sshLocByIp);
        }
    }

    @Override
    public PersistenceObjectStore newPersistenceObjectStore(String container) {
        return new JcloudsBlobStoreBasedObjectStore(this, container);
    }




    // ------------ static converters (could go to a new file) ------------------

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    public static File asFile(Object o) {
        if (o instanceof File) return (File)o;
        if (o == null) return null;
        return new File(o.toString());
    }

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    public static String fileAsString(Object o) {
        if (o instanceof String) return (String)o;
        if (o instanceof File) return ((File)o).getAbsolutePath();
        if (o==null) return null;
        return o.toString();
    }

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    protected static double toDouble(Object v) {
        if (v instanceof Number) {
            return ((Number)v).doubleValue();
        } else {
            throw new IllegalArgumentException("Invalid type for double: "+v+" of type "+v.getClass());
        }
    }

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    protected static String[] toStringArray(Object v) {
        return Strings.toStringList(v).toArray(new String[0]);
    }

    /** @deprecated since 0.11.0 use {@link Strings#toStringList(Object)} instead */
    @Deprecated
    protected static List<String> toListOfStrings(Object v) {
        return Strings.toStringList(v);
    }

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    protected static byte[] toByteArray(Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        } else if (v instanceof CharSequence) {
            return v.toString().getBytes();
        } else {
            throw new IllegalArgumentException("Invalid type for byte[]: "+v+" of type "+v.getClass());
        }
    }

    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    @VisibleForTesting
    static int[] toIntPortArray(Object v) {
        PortRange portRange = PortRanges.fromIterable(Collections.singletonList(v));
        return ArrayUtils.toPrimitive(Iterables.toArray(portRange, Integer.class));
    }


    /** @deprecated since 0.11.0 without replacement */
    @Deprecated
    // Handles GString
    protected static Map<String,String> toMapStringString(Object v) {
        if (v instanceof Map<?,?>) {
            Map<String,String> result = Maps.newLinkedHashMap();
            for (Map.Entry<?,?> entry : ((Map<?,?>)v).entrySet()) {
                String key = entry.getKey().toString();
                String value = entry.getValue().toString();
                result.put(key, value);
            }
            return result;
        } else if (v instanceof CharSequence) {
            return KeyValueParser.parseMap(v.toString());
        } else {
            throw new IllegalArgumentException("Invalid type for Map<String,String>: " + v +
                    (v != null ? " of type "+v.getClass() : ""));
        }
    }

    // TODO Very similar to EntityConfigMap.deepMerge
    private <T> Maybe<?> shallowMerge(Maybe<? extends T> val1, Maybe<? extends T> val2, ConfigKey<?> keyForLogging) {
        if (val2.isAbsent() || val2.isNull()) {
            return val1;
        } else if (val1.isAbsent()) {
            return val2;
        } else if (val1.isNull()) {
            return val1; // an explicit null means an override; don't merge
        } else if (val1.get() instanceof Map && val2.get() instanceof Map) {
            return Maybe.of(CollectionMerger.builder().deep(false).build().merge((Map<?,?>)val1.get(), (Map<?,?>)val2.get()));
        } else {
            // cannot merge; just return val1
            LOG.debug("Cannot merge values for "+keyForLogging.getName()+", because values are not maps: "+val1.get().getClass()+", and "+val2.get().getClass());
            return val1;
        }
    }
}
