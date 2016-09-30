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
import java.lang.reflect.Method;
import java.security.KeyPair;
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
import org.apache.brooklyn.core.BrooklynVersion;
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
import org.apache.brooklyn.core.mgmt.internal.LocalLocationManager;
import org.apache.brooklyn.core.mgmt.persist.LocationWithObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.persist.jclouds.JcloudsBlobStoreBasedObjectStore;
import org.apache.brooklyn.location.jclouds.networking.JcloudsPortForwarderExtension;
import org.apache.brooklyn.location.jclouds.templates.PortableTemplateBuilder;
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
import org.apache.brooklyn.util.core.crypto.SecureKeys;
import org.apache.brooklyn.util.core.flags.MethodCoercions;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
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
import org.apache.brooklyn.util.javalang.Enums;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.net.Cidr;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.Protocol;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.ssh.IptablesCommands;
import org.apache.brooklyn.util.ssh.IptablesCommands.Chain;
import org.apache.brooklyn.util.ssh.IptablesCommands.Policy;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.KeyValueParser;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.lang3.ArrayUtils;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.cloudstack.compute.options.CloudStackTemplateOptions;
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
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.jclouds.compute.functions.Sha512Crypt;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.rest.AuthorizationException;
import org.jclouds.scriptbuilder.domain.LiteralStatement;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import org.jclouds.scriptbuilder.functions.InitAdminAccess;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;
import org.jclouds.scriptbuilder.statements.login.ReplaceShadowPasswordEntry;
import org.jclouds.scriptbuilder.statements.ssh.AuthorizeRSAPublicKeys;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.jclouds.util.Predicates2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
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
import com.google.common.io.Files;
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
    static final String AWS_VPC_HELP_URL = "http://brooklyn.apache.org/v/"+BrooklynVersion.get()+"/ops/locations/more-clouds.html";

    private final AtomicBoolean loggedSshKeysHint = new AtomicBoolean(false);
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
     * @see {@link #isWindows(Image, ConfigBag)}
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
        if (result == 0) {
            return true;
        }
        
        return false;
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

    protected Collection<JcloudsLocationCustomizer> getCustomizers(ConfigBag setup) {
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

    protected MachineLocation obtainOnce(ConfigBag setup) throws NoMachinesAvailableException {
        AccessController.Response access = getManagementContext().getAccessController().canProvisionLocation(this);
        if (!access.isAllowed()) {
            throw new IllegalStateException("Access controller forbids provisioning in "+this+": "+access.getMsg());
        }

        boolean waitForSshable = !"false".equalsIgnoreCase(setup.get(WAIT_FOR_SSHABLE));
        boolean waitForWinRmable = !"false".equalsIgnoreCase(setup.get(WAIT_FOR_WINRM_AVAILABLE));
        boolean usePortForwarding = setup.get(USE_PORT_FORWARDING);
        boolean skipJcloudsSshing = Boolean.FALSE.equals(setup.get(USE_JCLOUDS_SSH_INIT)) || usePortForwarding;
        JcloudsPortForwarderExtension portForwarder = setup.get(PORT_FORWARDER);
        if (usePortForwarding) checkNotNull(portForwarder, "portForwarder, when use-port-forwarding enabled");

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
            LOG.info("Creating VM "+setup.getDescription()+" in "+this);

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
            try {
                // Setup the template
                template = buildTemplate(computeService, setup);
                boolean expectWindows = isWindows(template, setup);
                if (!skipJcloudsSshing) {
                    if (expectWindows) {
                        // TODO Was this too early to look at template.getImage? e.g. customizeTemplate could subsequently modify it.
                        LOG.warn("Ignoring invalid configuration for Windows provisioning of "+template.getImage()+": "+USE_JCLOUDS_SSH_INIT.getName()+" should be false");
                        skipJcloudsSshing = true;
                    } else if (waitForSshable) {
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
                
                customizeTemplate(setup, computeService, template);
                
                LOG.debug("jclouds using template {} / options {} to provision machine in {}",
                        new Object[] {template, template.getOptions(), setup.getDescription()});

                // no longer supported because config is sealed, we use an underlying config map
//                if (!setup.getUnusedConfig().isEmpty())
//                    if (LOG.isDebugEnabled())
//                        LOG.debug("NOTE: unused flags passed to obtain VM in "+setup.getDescription()+": "
//                                + Sanitizer.sanitize(setup.getUnusedConfig()));
                nodes = computeService.createNodesInGroup(groupId, 1, template);
                provisionTimestamp = Duration.of(provisioningStopwatch);
            } finally {
                machineCreationSemaphore.release();
            }

            node = Iterables.getOnlyElement(nodes, null);
            LOG.debug("jclouds created {} for {}", node, setup.getDescription());
            if (node == null)
                throw new IllegalStateException("No nodes returned by jclouds create-nodes in " + setup.getDescription());

            boolean windows = isWindows(node, setup);
            if (windows) {
                int newLoginPort = node.getLoginPort() == 22 ? (getConfig(WinRmMachineLocation.USE_HTTPS_WINRM) ? 5986 : 5985) : node.getLoginPort();
                String newLoginUser = "root".equals(node.getCredentials().getUser()) ? "Administrator" : node.getCredentials().getUser();
                LOG.debug("jclouds created Windows VM {}; transforming connection details: loginPort from {} to {}; loginUser from {} to {}", 
                        new Object[] {node, node.getLoginPort(), newLoginPort, node.getCredentials().getUser(), newLoginUser});
                
                node = NodeMetadataBuilder.fromNodeMetadata(node)
                        .loginPort(newLoginPort)
                        .credentials(LoginCredentials.builder(node.getCredentials()).user(newLoginUser).build())
                        .build();
            }
            // FIXME How do we influence the node.getLoginPort, so it is set correctly for Windows?
            // Setup port-forwarding, if required
            Optional<HostAndPort> sshHostAndPortOverride;
            if (usePortForwarding) {
                sshHostAndPortOverride = Optional.of(portForwarder.openPortForwarding(
                        node,
                        node.getLoginPort(),
                        Optional.<Integer>absent(),
                        Protocol.TCP,
                        Cidr.UNIVERSAL));
            } else {
                sshHostAndPortOverride = Optional.absent();
            }

            LoginCredentials initialCredentials = node.getCredentials();
            if (skipJcloudsSshing) {
                boolean waitForConnectable = (windows) ? waitForWinRmable : waitForSshable;
                if (waitForConnectable) {
                    if (windows) {
                        // TODO Does jclouds support any windows user setup?
                        initialCredentials = waitForWinRmAvailable(computeService, node, sshHostAndPortOverride, setup);
                    } else {
                        initialCredentials = waitForSshable(computeService, node, sshHostAndPortOverride, setup);
                    }
                    userCredentials = createUser(computeService, node, sshHostAndPortOverride, initialCredentials, setup);
                }
            }

            // Figure out which login-credentials to use
            LoginCredentials customCredentials = setup.get(CUSTOM_CREDENTIALS);
            if (customCredentials != null) {
                userCredentials = customCredentials;
                //set userName and other data, from these credentials
                Object oldUsername = setup.put(USER, customCredentials.getUser());
                LOG.debug("node {} username {} / {} (customCredentials)", new Object[] { node, customCredentials.getUser(), oldUsername });
                if (customCredentials.getOptionalPassword().isPresent()) setup.put(PASSWORD, customCredentials.getOptionalPassword().get());
                if (customCredentials.getOptionalPrivateKey().isPresent()) setup.put(PRIVATE_KEY_DATA, customCredentials.getOptionalPrivateKey().get());
            }
            if (userCredentials == null || (!userCredentials.getOptionalPassword().isPresent() && !userCredentials.getOptionalPrivateKey().isPresent())) {
                // We either don't have any userCredentials, or it is missing both a password/key.
                // TODO See waitForSshable, which now handles if the node.getLoginCredentials has both a password+key
                userCredentials = extractVmCredentials(setup, node, initialCredentials);
            }
            if (userCredentials == null) {
                // TODO See waitForSshable, which now handles if the node.getLoginCredentials has both a password+key
                userCredentials = extractVmCredentials(setup, node, initialCredentials);
            }
            if (userCredentials != null) {
                node = NodeMetadataBuilder.fromNodeMetadata(node).credentials(userCredentials).build();
            } else {
                // only happens if something broke above...
                userCredentials = LoginCredentials.fromCredentials(node.getCredentials());
            }
            // store the credentials, in case they have changed
            putIfPresentButDifferent(setup, JcloudsLocationConfig.PASSWORD, userCredentials.getOptionalPassword().orNull());
            putIfPresentButDifferent(setup, JcloudsLocationConfig.PRIVATE_KEY_DATA, userCredentials.getOptionalPrivateKey().orNull());

            // Wait for the VM to be reachable over SSH
            if (waitForSshable && !windows) {
                waitForSshable(computeService, node, sshHostAndPortOverride, ImmutableList.of(userCredentials), setup);
            } else {
                LOG.debug("Skipping ssh check for {} ({}) due to config waitForSshable=false", node, setup.getDescription());
            }

            // Do not store the credentials on the node as this may leak the credentials if they
            // are obtained from an external supplier
            node = NodeMetadataBuilder.fromNodeMetadata(node).credentials(null).build();

            usableTimestamp = Duration.of(provisioningStopwatch);

            // Create a JcloudsSshMachineLocation, and register it
            if (windows) {
                machineLocation = registerWinRmMachineLocation(computeService, node, userCredentials, sshHostAndPortOverride, setup);
            } else {
                machineLocation = registerJcloudsSshMachineLocation(computeService, node, Optional.fromNullable(template), userCredentials, sshHostAndPortOverride, setup);
            }

            PortForwardManager portForwardManager = setup.get(PORT_FORWARDING_MANAGER);
            if (portForwardManager == null) {
                LOG.debug("No PortForwardManager, using default");
                portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
            }

            if (usePortForwarding && sshHostAndPortOverride.isPresent()) {
                // Now that we have the sshMachineLocation, we can associate the port-forwarding address with it.
                portForwardManager.associate(node.getId(), sshHostAndPortOverride.get(), machineLocation, node.getLoginPort());
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
            if (waitForSshable) {

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
                                iptablesRules = createIptablesRulesForNetworkInterface(inboundPorts);
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
            for (JcloudsLocationCustomizer customizer : getCustomizers(setup)) {
                LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
                customizer.customize(this, computeService, machineLocation);
            }
            for (MachineLocationCustomizer customizer : getMachineCustomizers(setup)) {
                LOG.debug("Customizing machine {}, using customizer {}", machineLocation, customizer);
                customizer.customize(machineLocation);
            }

            customizedTimestamp = Duration.of(provisioningStopwatch);

            try {
                String logMessage = "Finished VM "+setup.getDescription()+" creation:"
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
            } catch (Exception e){
                // TODO Remove try-catch! @Nakomis: why did you add it? What exception happened during logging?
                Exceptions.propagateIfFatal(e);
                LOG.warn("Problem generating log message summarising completion of jclouds machine provisioning "+machineLocation+" by "+this, e);
            }

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
            
            LOG.error("Failed to start VM for "+setup.getDescription() + (destroyNode ? " (destroying)" : "")
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

    private static interface CustomizeTemplateBuilder {
        void apply(TemplateBuilder tb, ConfigBag props, Object v);
    }

    public static interface CustomizeTemplateOptions {
        void apply(TemplateOptions tb, ConfigBag props, Object v);
    }

    /** properties which cause customization of the TemplateBuilder */
    public static final Map<ConfigKey<?>,CustomizeTemplateBuilder> SUPPORTED_TEMPLATE_BUILDER_PROPERTIES = ImmutableMap.<ConfigKey<?>,CustomizeTemplateBuilder>builder()
            .put(OS_64_BIT, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        Boolean os64Bit = TypeCoercions.coerce(v, Boolean.class);
                        if (os64Bit!=null)
                            tb.os64Bit(os64Bit);
                    }})
            .put(MIN_RAM, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.minRam( (int)(ByteSizeStrings.parse(Strings.toString(v), "mb")/1000/1000) );
                    }})
            .put(MIN_CORES, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.minCores(TypeCoercions.coerce(v, Double.class));
                    }})
            .put(MIN_DISK, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.minDisk( (int)(ByteSizeStrings.parse(Strings.toString(v), "gb")/1000/1000/1000) );
                    }})
            .put(HARDWARE_ID, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.hardwareId(((CharSequence)v).toString());
                    }})
            .put(IMAGE_ID, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.imageId(((CharSequence)v).toString());
                    }})
            .put(IMAGE_DESCRIPTION_REGEX, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.imageDescriptionMatches(((CharSequence)v).toString());
                    }})
            .put(IMAGE_NAME_REGEX, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.imageNameMatches(((CharSequence)v).toString());
                    }})
            .put(OS_FAMILY, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        Maybe<OsFamily> osFamily = Enums.valueOfIgnoreCase(OsFamily.class, v.toString());
                        if (osFamily.isAbsent())
                            throw new IllegalArgumentException("Invalid "+OS_FAMILY+" value "+v);
                        tb.osFamily(osFamily.get());
                    }})
            .put(OS_VERSION_REGEX, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.osVersionMatches( ((CharSequence)v).toString() );
                    }})
            .put(TEMPLATE_SPEC, new CustomizeTemplateBuilder() {
                public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        tb.from(TemplateBuilderSpec.parse(((CharSequence)v).toString()));
                    }})
            .put(DEFAULT_IMAGE_ID, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        /* done in the code, but included here so that it is in the map */
                    }})
            .put(TEMPLATE_BUILDER, new CustomizeTemplateBuilder() {
                    public void apply(TemplateBuilder tb, ConfigBag props, Object v) {
                        /* done in the code, but included here so that it is in the map */
                    }})
            .build();

    /** properties which cause customization of the TemplateOptions */
    public static final Map<ConfigKey<?>,CustomizeTemplateOptions> SUPPORTED_TEMPLATE_OPTIONS_PROPERTIES = ImmutableMap.<ConfigKey<?>,CustomizeTemplateOptions>builder()
            .put(SECURITY_GROUPS, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof EC2TemplateOptions) {
                            String[] securityGroups = toStringArray(v);
                            ((EC2TemplateOptions)t).securityGroups(securityGroups);
                        } else if (t instanceof NovaTemplateOptions) {
                            String[] securityGroups = toStringArray(v);
                            ((NovaTemplateOptions)t).securityGroups(securityGroups);
                        } else if (t instanceof SoftLayerTemplateOptions) {
                            String[] securityGroups = toStringArray(v);
                            ((SoftLayerTemplateOptions)t).securityGroups(securityGroups);
                        } else if (isGoogleComputeTemplateOptions(t)) {
                            String[] securityGroups = toStringArray(v);
                            t.securityGroups(securityGroups);
                        } else {
                            LOG.info("ignoring securityGroups({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
                        }
                    }})
            .put(INBOUND_PORTS, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        int[] inboundPorts = toIntPortArray(v);
                        if (LOG.isDebugEnabled()) LOG.debug("opening inbound ports {} for cloud/type {}", Arrays.toString(inboundPorts), t.getClass());
                        t.inboundPorts(inboundPorts);
                    }})
            .put(USER_METADATA_STRING, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof EC2TemplateOptions) {
                            // See AWS docs: http://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/UsingConfig_WinAMI.html#user-data-execution
                            if (v==null) return;
                            String data = v.toString();
                            if (!(data.startsWith("<script>") || data.startsWith("<powershell>"))) {
                                data = "<script> " + data + " </script>";
                            }
                            ((EC2TemplateOptions)t).userData(data.getBytes());
                        } else if (t instanceof SoftLayerTemplateOptions) {
                            ((SoftLayerTemplateOptions)t).userData(Strings.toString(v));
                        } else {
                            // Try reflection: userData(String), or guestCustomizationScript(String);
                            // the latter is used by vCloud Director.
                            Class<? extends TemplateOptions> clazz = t.getClass();
                            Method userDataMethod = null;
                            try {
                                userDataMethod = clazz.getMethod("userData", String.class);
                            } catch (SecurityException e) {
                                LOG.info("Problem reflectively inspecting methods of "+t.getClass()+" for setting userData", e);
                            } catch (NoSuchMethodException e) {
                                try {
                                    // For vCloud Director
                                    userDataMethod = clazz.getMethod("guestCustomizationScript", String.class);
                                } catch (NoSuchMethodException e2) {
                                    // expected on various other clouds
                                }
                            }
                            if (userDataMethod != null) {
                                try {
                                    userDataMethod.invoke(t, Strings.toString(v));
                                } catch (InvocationTargetException e) {
                                    LOG.info("Problem invoking "+userDataMethod.getName()+" of "+t.getClass()+", for setting userData (rethrowing)", e);
                                    throw Exceptions.propagate(e);
                                } catch (IllegalAccessException e) {
                                    LOG.debug("Unable to reflectively invoke "+userDataMethod.getName()+" of "+t.getClass()+", for setting userData (rethrowing)", e);
                                    throw Exceptions.propagate(e);
                                }
                            } else {
                                LOG.info("ignoring userDataString({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
                            }
                        }
                    }})
            .put(USER_DATA_UUENCODED, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof EC2TemplateOptions) {
                            byte[] bytes = toByteArray(v);
                            ((EC2TemplateOptions)t).userData(bytes);
                        } else if (t instanceof SoftLayerTemplateOptions) {
                            ((SoftLayerTemplateOptions)t).userData(Strings.toString(v));
                        } else {
                            LOG.info("ignoring userData({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
                        }
                    }})
            .put(STRING_TAGS, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        List<String> tags = toListOfStrings(v);
                        if (LOG.isDebugEnabled()) LOG.debug("setting VM tags {} for {}", tags, t);
                        t.tags(tags);
                    }})
            .put(USER_METADATA_MAP, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (v != null) {
                            t.userMetadata(toMapStringString(v));
                        }
                    }})
            .put(EXTRA_PUBLIC_KEY_DATA_TO_AUTH, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        // this is unreliable: 
                        // * seems now (Aug 2016) to be run *before* the TO.runScript which creates the user,
                        // so is installed for the initial login user not the created user
                        // * not supported in GCE (it uses it as the login public key, see email to jclouds list, 29 Aug 2015)
                        // so only works if you also overrideLoginPrivateKey
                        // --
                        // for this reason we also inspect these ourselves 
                        // along with EXTRA_PUBLIC_KEY_URLS_TO_AUTH
                        // and install after creation;
                        // --
                        // we also do it here for legacy reasons though i (alex) can't think of any situations it's needed
                        // --
                        // also we warn on exceptions in case someone is dumping comments or something else
                        try {
                            t.authorizePublicKey(((CharSequence)v).toString());
                        } catch (Exception e) {
                            Exceptions.propagateIfFatal(e);
                            LOG.warn("Error trying jclouds authorizePublicKey; will run later: "+e, e);
                        }
                    }})
            .put(RUN_AS_ROOT, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        t.runAsRoot((Boolean)v);
                    }})
            .put(LOGIN_USER, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (v != null) {
                            t.overrideLoginUser(((CharSequence)v).toString());
                        }
                    }})
            .put(LOGIN_USER_PASSWORD, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (v != null) {
                            t.overrideLoginPassword(((CharSequence)v).toString());
                        }
                    }})
            .put(LOGIN_USER_PRIVATE_KEY_FILE, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (v != null) {
                            String privateKeyFileName = ((CharSequence)v).toString();
                            String privateKey;
                            try {
                                privateKey = Files.toString(new File(Os.tidyPath(privateKeyFileName)), Charsets.UTF_8);
                            } catch (IOException e) {
                                LOG.error(privateKeyFileName + "not found", e);
                                throw Exceptions.propagate(e);
                            }
                            t.overrideLoginPrivateKey(privateKey);
                        }
                    }})
            .put(LOGIN_USER_PRIVATE_KEY_DATA, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (v != null) {
                            t.overrideLoginPrivateKey(((CharSequence)v).toString());
                        }
                    }})
            .put(KEY_PAIR, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof EC2TemplateOptions) {
                            ((EC2TemplateOptions)t).keyPair(((CharSequence)v).toString());
                        } else if (t instanceof NovaTemplateOptions) {
                            ((NovaTemplateOptions)t).keyPairName(((CharSequence)v).toString());
                        } else if (t instanceof CloudStackTemplateOptions) {
                            ((CloudStackTemplateOptions) t).keyPair(((CharSequence) v).toString());
                        } else {
                            LOG.info("ignoring keyPair({}) in VM creation because not supported for cloud/type ({})", v, t);
                        }
                    }})
            .put(AUTO_GENERATE_KEYPAIRS, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof NovaTemplateOptions) {
                            ((NovaTemplateOptions)t).generateKeyPair((Boolean)v);
                        } else if (t instanceof CloudStackTemplateOptions) {
                            ((CloudStackTemplateOptions) t).generateKeyPair((Boolean) v);
                        } else {
                            LOG.info("ignoring auto-generate-keypairs({}) in VM creation because not supported for cloud/type ({})", v, t);
                        }
                    }})
            .put(AUTO_CREATE_FLOATING_IPS, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        LOG.warn("Using deprecated "+AUTO_CREATE_FLOATING_IPS+"; use "+AUTO_ASSIGN_FLOATING_IP+" instead");
                        if (t instanceof NovaTemplateOptions) {
                            ((NovaTemplateOptions)t).autoAssignFloatingIp((Boolean)v);
                        } else {
                            LOG.info("ignoring auto-generate-floating-ips({}) in VM creation because not supported for cloud/type ({})", v, t);
                        }
                    }})
            .put(AUTO_ASSIGN_FLOATING_IP, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof NovaTemplateOptions) {
                            ((NovaTemplateOptions)t).autoAssignFloatingIp((Boolean)v);
                        } else if (t instanceof CloudStackTemplateOptions) {
                            ((CloudStackTemplateOptions)t).setupStaticNat((Boolean)v);
                        } else {
                            LOG.info("ignoring auto-assign-floating-ip({}) in VM creation because not supported for cloud/type ({})", v, t);
                        }
                    }})
            .put(NETWORK_NAME, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof AWSEC2TemplateOptions) {
                            // subnet ID is the sensible interpretation of network name in EC2
                            ((AWSEC2TemplateOptions)t).subnetId((String)v);
                            
                        } else {
                            if (isGoogleComputeTemplateOptions(t)) {
                                // no warning needed
                                // we think this is the only jclouds endpoint which supports this option
                                
                            } else if (t instanceof SoftLayerTemplateOptions) {
                                LOG.warn("networkName is not be supported in SoftLayer; use `templateOptions` with `primaryNetworkComponentNetworkVlanId` or `primaryNetworkBackendComponentNetworkVlanId`");
                            } else if (!(t instanceof CloudStackTemplateOptions) && !(t instanceof NovaTemplateOptions)) {
                                LOG.warn("networkName is experimental in many jclouds endpoints may not be supported in this cloud");
                                // NB, from @andreaturli
//                                Cloudstack uses custom securityGroupIds and networkIds not the generic networks
//                                Openstack Nova uses securityGroupNames which is marked as @deprecated (suggests to use groups which is maybe even more confusing)
//                                Azure supports the custom networkSecurityGroupName
                            }
                            
                            t.networks((String)v);
                        }
                    }})
            .put(DOMAIN_NAME, new CustomizeTemplateOptions() {
                    public void apply(TemplateOptions t, ConfigBag props, Object v) {
                        if (t instanceof SoftLayerTemplateOptions) {
                            ((SoftLayerTemplateOptions)t).domainName(TypeCoercions.coerce(v, String.class));
                        } else {
                            LOG.info("ignoring domain-name({}) in VM creation because not supported for cloud/type ({})", v, t);                            
                        }
                    }})
            .put(TEMPLATE_OPTIONS, new CustomizeTemplateOptions() {
                @Override
                public void apply(TemplateOptions options, ConfigBag config, Object v) {
                    if (v == null) return;
                    @SuppressWarnings("unchecked") Map<String, Object> optionsMap = (Map<String, Object>) v;
                    if (optionsMap.isEmpty()) return;

                    Class<? extends TemplateOptions> clazz = options.getClass();
                    for(final Map.Entry<String, Object> option : optionsMap.entrySet()) {
                        if (option.getValue() != null) {
                            Maybe<?> result = MethodCoercions.tryFindAndInvokeBestMatchingMethod(options, option.getKey(), option.getValue());
                            if(result.isAbsent()) {
                                LOG.warn("Ignoring request to set template option {} because this is not supported by {}", new Object[] { option.getKey(), clazz.getCanonicalName() });
                            }
                        } else {
                            // jclouds really doesn't like you to pass nulls; don't do it! For us,
                            // null is the only way to remove an inherited value when the templateOptions
                            // map is being merged.
                            LOG.debug("Ignoring request to set template option {} because value is null", new Object[] { option.getKey(), clazz.getCanonicalName() });
                        }
                    }
                }})
            .build();

    /**
     * Avoid having a dependency on googlecompute because it doesn't have an OSGi bundle yet.
     * Fixed in jclouds 2.0.0-SNAPSHOT
     */
    private static boolean isGoogleComputeTemplateOptions(TemplateOptions t) {
        return t.getClass().getName().equals("org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions");
    }

    /** hook whereby template customizations can be made for various clouds */
    protected void customizeTemplate(ConfigBag setup, ComputeService computeService, Template template) {
        for (JcloudsLocationCustomizer customizer : getCustomizers(setup)) {
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
    public Template buildTemplate(ComputeService computeService, ConfigBag config) {
        TemplateBuilder templateBuilder = (TemplateBuilder) config.get(TEMPLATE_BUILDER);
        if (templateBuilder==null) {
            templateBuilder = new PortableTemplateBuilder<PortableTemplateBuilder<?>>();
        } else {
            LOG.debug("jclouds using templateBuilder {} as custom base for provisioning in {} for {}", new Object[] {
                    templateBuilder, this, config.getDescription()});
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
                + "it is recommended to use a PortableTemplateBuilder if you supply a TemplateBuilder", config.getDescription());
        }

        if (!Strings.isEmpty(config.get(CLOUD_REGION_ID))) {
            templateBuilder.locationId(config.get(CLOUD_REGION_ID));
        }

        // Apply the template builder and options properties
        for (Map.Entry<ConfigKey<?>, CustomizeTemplateBuilder> entry : SUPPORTED_TEMPLATE_BUILDER_PROPERTIES.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            Object val = config.containsKey(key) ? config.get(key) : key.getDefaultValue();
            if (val != null) {
                CustomizeTemplateBuilder code = entry.getValue();
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
        for (JcloudsLocationCustomizer customizer : getCustomizers(config)) {
            customizer.customize(this, computeService, templateBuilder);
        }

        LOG.debug("jclouds using templateBuilder {} for provisioning in {} for {}", new Object[] {
            templateBuilder, this, config.getDescription()});

        // Finally try to build the template
        Template template = null;
        Image image;
        try {
            template = templateBuilder.build();
            if (template==null) throw new IllegalStateException("No matching template; check image and hardware constraints (e.g. OS, RAM); using "+templateBuilder);
            image = template.getImage();
            LOG.debug("jclouds found template "+template+" (image "+image+") for provisioning in "+this+" for "+config.getDescription());
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
               
        for (Map.Entry<ConfigKey<?>, CustomizeTemplateOptions> entry : SUPPORTED_TEMPLATE_OPTIONS_PROPERTIES.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            CustomizeTemplateOptions code = entry.getValue();
            if (config.containsKey(key) && config.get(key) != null) {
                code.apply(options, config, config.get(key));
            }
        }

        return template;
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
    protected LoginCredentials createUser(ComputeService computeService, NodeMetadata node, Optional<HostAndPort> hostAndPortOverride, LoginCredentials initialCredentials, ConfigBag config) {
        Image image = (node.getImageId() != null) ? computeService.getImage(node.getImageId()) : null;
        UserCreation userCreation = createUserStatements(image, config);

        if (!userCreation.statements.isEmpty()) {
            // If unsure of OS family, default to unix for rendering statements.
            org.jclouds.scriptbuilder.domain.OsFamily scriptOsFamily;
            if (isWindows(node, config)) {
                scriptOsFamily = org.jclouds.scriptbuilder.domain.OsFamily.WINDOWS;
            } else {
                scriptOsFamily = org.jclouds.scriptbuilder.domain.OsFamily.UNIX;
            }

            boolean windows = isWindows(node, config);

            if (windows) {
                LOG.warn("Unable to execute statements on WinRM in JcloudsLocation; skipping for "+node+": "+userCreation.statements);
                
            } else {
                List<String> commands = Lists.newArrayList();
                for (Statement statement : userCreation.statements) {
                    InitAdminAccess initAdminAccess = new InitAdminAccess(new AdminAccessConfiguration.Default());
                    initAdminAccess.visit(statement);
                    commands.add(statement.render(scriptOsFamily));
                }

                String initialUser = initialCredentials.getUser();
                String address = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getHostText() : getFirstReachableAddress(node, config);
                int port = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getPort() : node.getLoginPort();
                
                // TODO Retrying lots of times as workaround for vcloud-director. There the guest customizations
                // can cause the VM to reboot shortly after it was ssh'able.
                Map<String,Object> execProps = Maps.newLinkedHashMap();
                execProps.put(ShellTool.PROP_RUN_AS_ROOT.getName(), true);
                execProps.put(SshTool.PROP_SSH_TRIES.getName(), 50);
                execProps.put(SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), 10*60*1000);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("VM {}: executing user creation/setup via {}@{}:{}; commands: {}", new Object[] {
                            config.getDescription(), initialUser, address, port, commands});
                }

                HostAndPort hostAndPort = hostAndPortOverride.isPresent() ? hostAndPortOverride.get() : HostAndPort.fromParts(address, port);
                SshMachineLocation sshLoc = createTemporarySshMachineLocation(hostAndPort, initialCredentials, config);
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

        return userCreation.createdUserCredentials;
    }

    /**
     * Setup the TemplateOptions to create the user.
     */
    protected LoginCredentials initTemplateForCreateUser(Template template, ConfigBag config) {
        UserCreation userCreation = createUserStatements(template.getImage(), config);

        if (userCreation.statements.size() > 0) {
            TemplateOptions options = template.getOptions();
            options.runScript(new StatementList(userCreation.statements));
        }

        return userCreation.createdUserCredentials;
    }

    protected static class UserCreation {
        public final LoginCredentials createdUserCredentials;
        public final List<Statement> statements;

        public UserCreation(LoginCredentials creds, List<Statement> statements) {
            this.createdUserCredentials = creds;
            this.statements = statements;
        }
    }

    /**
     * Returns the commands required to create the user, to be used for connecting (e.g. over ssh)
     * to the machine; also returns the expected login credentials.
     * <p>
     * The returned login credentials may be null if we haven't done any user-setup and no specific
     * user was supplied (i.e. if {@code dontCreateUser} was true and {@code user} was null or blank).
     * In which case, the caller should use the jclouds node's login credentials.
     * <p>
     * There are quite a few configuration options. Depending on their values, the user-creation
     * behaves differently:
     * <ul>
     *   <li>{@code dontCreateUser} says not to run any user-setup commands at all. If {@code user} is
     *       non-empty (including with the default value), then that user will subsequently be used,
     *       otherwise the (inferred) {@code loginUser} will be used.
     *   <li>{@code loginUser} refers to the existing user that jclouds should use when setting up the VM.
     *       Normally this will be inferred from the image (i.e. doesn't need to be explicitly set), but sometimes
     *       the image gets it wrong so this can be a handy override.
     *   <li>{@code user} is the username for brooklyn to subsequently use when ssh'ing to the machine.
     *       If not explicitly set, its value will default to the username of the user running brooklyn.
     *       <ul>
     *         <li>If the {@code user} value is null or empty, then the (inferred) {@code loginUser} will
     *             subsequently be used, setting up the password/authorizedKeys for that loginUser.
     *         <li>If the {@code user} is "root", then setup the password/authorizedKeys for root.
     *         <li>If the {@code user} equals the (inferred) {@code loginUser}, then don't try to create this
     *             user but instead just setup the password/authorizedKeys for the user.
     *         <li>Otherwise create the given user, setting up the password/authorizedKeys (unless
     *             {@code dontCreateUser} is set, obviously).
     *       </ul>
     *   <li>{@code publicKeyData} is the key to authorize (i.e. add to .ssh/authorized_keys),
     *       if not null or blank. Note the default is to use {@code ~/.ssh/id_rsa.pub} or {@code ~/.ssh/id_dsa.pub}
     *       if either of those files exist for the user running brooklyn.
     *       Related is {@code publicKeyFile}, which is used to populate publicKeyData.
     *   <li>{@code password} is the password to set for the user. If null or blank, then a random password
     *       will be auto-generated and set.
     *   <li>{@code privateKeyData} is the key to use when subsequent ssh'ing, if not null or blank.
     *       Note the default is to use {@code ~/.ssh/id_rsa} or {@code ~/.ssh/id_dsa}.
     *       The subsequent preferences for ssh'ing are:
     *       <ul>
     *         <li>Use the {@code privateKeyData} if not null or blank (including if using default)
     *         <li>Use the {@code password} (or the auto-generated password if that is blank).
     *       </ul>
     *   <li>{@code grantUserSudo} determines whether or not the created user may run the sudo command.</li>
     * </ul>
     *
     * @param image  The image being used to create the VM
     * @param config Configuration for creating the VM
     * @return       The commands required to create the user, along with the expected login credentials for that user,
     * or null if we are just going to use those from jclouds.
     */
    protected UserCreation createUserStatements(@Nullable Image image, ConfigBag config) {
        //NB: private key is not installed remotely, just used to get/validate the public key

        boolean windows = isWindows(image, config);
        String user = getUser(config);
        String explicitLoginUser = config.get(LOGIN_USER);
        String loginUser = groovyTruth(explicitLoginUser) ? explicitLoginUser : (image != null && image.getDefaultCredentials() != null) ? image.getDefaultCredentials().identity : null;
        boolean dontCreateUser = config.get(DONT_CREATE_USER);
        boolean grantUserSudo = config.get(GRANT_USER_SUDO);
        OsCredential credential = LocationConfigUtils.getOsCredential(config);
        credential.checkNoErrors().logAnyWarnings();
        String passwordToSet = Strings.isNonBlank(credential.getPassword()) ? credential.getPassword() : Identifiers.makeRandomId(12);
        List<Statement> statements = Lists.newArrayList();
        LoginCredentials createdUserCreds = null;

        if (dontCreateUser) {
            // dontCreateUser:
            // if caller has not specified a user, we'll just continue to use the loginUser;
            // if caller *has*, we set up our credentials assuming that user and credentials already exist

            if (Strings.isBlank(user)) {
                // createdUserCreds returned from this method will be null;
                // we will use the creds returned by jclouds on the node
                LOG.info("Not setting up any user (subsequently using loginUser {})", user, loginUser);
                config.put(USER, loginUser);

            } else {
                LOG.info("Not creating user {}, and not installing its password or authorizing keys (assuming it exists)", user);

                if (credential.isUsingPassword()) {
                    createdUserCreds = LoginCredentials.builder().user(user).password(credential.getPassword()).build();
                    if (Boolean.FALSE.equals(config.get(DISABLE_ROOT_AND_PASSWORD_SSH))) {
                        statements.add(org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
                    }
                } else if (credential.hasKey()) {
                    createdUserCreds = LoginCredentials.builder().user(user).privateKey(credential.getPrivateKeyData()).build();
                }
            }

        } else if (windows) {
            // TODO Generate statements to create the user.
            // createdUserCreds returned from this method will be null;
            // we will use the creds returned by jclouds on the node
            LOG.warn("Not creating or configuring user on Windows VM, despite "+DONT_CREATE_USER.getName()+" set to false");

            // TODO extractVmCredentials() will use user:publicKeyData defaults, if we don't override this.
            // For linux, how would we configure Brooklyn to use the node.getCredentials() - i.e. the version
            // that the cloud automatically generated?
            if (config.get(USER) != null) config.put(USER, "");
            if (config.get(PASSWORD) != null) config.put(PASSWORD, "");
            if (config.get(PRIVATE_KEY_DATA) != null) config.put(PRIVATE_KEY_DATA, "");
            if (config.get(PRIVATE_KEY_FILE) != null) config.put(PRIVATE_KEY_FILE, "");
            if (config.get(PUBLIC_KEY_DATA) != null) config.put(PUBLIC_KEY_DATA, "");
            if (config.get(PUBLIC_KEY_FILE) != null) config.put(PUBLIC_KEY_FILE, "");

        } else if (Strings.isBlank(user) || user.equals(loginUser) || user.equals(ROOT_USERNAME)) {
            boolean useKey = Strings.isNonBlank(credential.getPublicKeyData());
            
            // For subsequent ssh'ing, we'll be using the loginUser
            if (Strings.isBlank(user)) {
                user = loginUser;
                config.put(USER, user);
            }

            // Using the pre-existing loginUser; setup the publicKey/password so can login as expected

            // *Always* change the password (unless dontCreateUser was specified)
            statements.add(new ReplaceShadowPasswordEntry(Sha512Crypt.function(), user, passwordToSet));
            createdUserCreds = LoginCredentials.builder().user(user).password(passwordToSet).build();

            if (useKey) {
                // NB: further keys are added from config *after* user creation
                statements.add(new AuthorizeRSAPublicKeys("~"+user+"/.ssh", ImmutableList.of(credential.getPublicKeyData())));
                if (Strings.isNonBlank(credential.getPrivateKeyData())) {
                    createdUserCreds = LoginCredentials.builder().user(user).privateKey(credential.getPrivateKeyData()).build();
                }
            }
            
            if (!useKey || Boolean.FALSE.equals(config.get(DISABLE_ROOT_AND_PASSWORD_SSH))) {
                // ensure password is permitted for ssh
                statements.add(org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
                if (user.equals(ROOT_USERNAME)) {
                    statements.add(org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig(ImmutableMap.of("PermitRootLogin", "yes")));
                }
            }

        } else {
            String pubKey = credential.getPublicKeyData();
            String privKey = credential.getPrivateKeyData();

            if (credential.isEmpty()) {
                /*
                 * TODO have an explicit `create_new_key_per_machine` config key.
                 * error if privateKeyData is set in this case.
                 * publicKeyData automatically added to EXTRA_SSH_KEY_URLS_TO_AUTH.
                 *
                 * if this config key is not set, use a key `brooklyn_id_rsa` and `.pub` in `MGMT_BASE_DIR`,
                 * with permission 0600, creating it if necessary, and logging the fact that this was created.
                 */
                if (!config.containsKey(PRIVATE_KEY_FILE) && loggedSshKeysHint.compareAndSet(false, true)) {
                    LOG.info("Default SSH keys not found or not usable; will create new keys for each machine. "
                        + "Create ~/.ssh/id_rsa or "
                        + "set "+PRIVATE_KEY_FILE.getName()+" / "+PRIVATE_KEY_PASSPHRASE.getName()+" / "+PASSWORD.getName()+" "
                        + "as appropriate for this location if you wish to be able to log in without Brooklyn.");
                }
                KeyPair newKeyPair = SecureKeys.newKeyPair();
                pubKey = SecureKeys.toPub(newKeyPair);
                privKey = SecureKeys.toPem(newKeyPair);
                LOG.debug("Brooklyn key being created for "+user+" at new machine "+this+" is:\n"+privKey);
            }
            // ensure credential is not used any more, as we have extracted all useful info
            credential = null;

            // Create the user
            // note AdminAccess requires _all_ fields set, due to http://code.google.com/p/jclouds/issues/detail?id=1095
            AdminAccess.Builder adminBuilder = AdminAccess.builder()
                    .adminUsername(user)
                    .grantSudoToAdminUser(grantUserSudo);
            adminBuilder.cryptFunction(Sha512Crypt.function());

            boolean useKey = Strings.isNonBlank(pubKey);
            adminBuilder.cryptFunction(Sha512Crypt.function());

            // always set this password; if not supplied, it will be a random string
            adminBuilder.adminPassword(passwordToSet);
            // log the password also, in case we need it
            LOG.debug("Password '"+passwordToSet+"' being created for user '"+user+"' at the machine we are about to provision in "+this+"; "+
                (useKey ? "however a key will be used to access it" : "this will be the only way to log in"));

            if (grantUserSudo && config.get(JcloudsLocationConfig.DISABLE_ROOT_AND_PASSWORD_SSH)) {
                // the default - set root password which we forget, because we have sudo acct
                // (and lock out root and passwords from ssh)
                adminBuilder.resetLoginPassword(true);
                adminBuilder.loginPassword(Identifiers.makeRandomId(12));
            } else {
                adminBuilder.resetLoginPassword(false);
                adminBuilder.loginPassword(Identifiers.makeRandomId(12)+"-ignored");
            }

            if (useKey) {
                adminBuilder.authorizeAdminPublicKey(true).adminPublicKey(pubKey);
            } else {
                adminBuilder.authorizeAdminPublicKey(false).adminPublicKey(Identifiers.makeRandomId(12)+"-ignored");
            }

            // jclouds wants us to give it the private key, otherwise it might refuse to authorize the public key
            // (in AdminAccess.build, if adminUsername != null && adminPassword != null);
            // we don't want to give it the private key, but we *do* want the public key authorized;
            // this code seems to trigger that.
            // (we build the creds below)
            adminBuilder.installAdminPrivateKey(false).adminPrivateKey(Identifiers.makeRandomId(12)+"-ignored");

            // lock SSH means no root login and no passwordless login
            // if we're using a password or we don't have sudo, then don't do this!
            adminBuilder.lockSsh(useKey && grantUserSudo && config.get(JcloudsLocationConfig.DISABLE_ROOT_AND_PASSWORD_SSH));

            statements.add(adminBuilder.build());

            if (useKey) {
                createdUserCreds = LoginCredentials.builder().user(user).privateKey(privKey).build();
            } else if (passwordToSet!=null) {
                createdUserCreds = LoginCredentials.builder().user(user).password(passwordToSet).build();
            }
            
            if (!useKey || Boolean.FALSE.equals(config.get(DISABLE_ROOT_AND_PASSWORD_SSH))) {
                // ensure password is permitted for ssh
                statements.add(org.jclouds.scriptbuilder.statements.ssh.SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
            }
        }

        String customTemplateOptionsScript = config.get(CUSTOM_TEMPLATE_OPTIONS_SCRIPT_CONTENTS);
        if (Strings.isNonBlank(customTemplateOptionsScript)) {
            statements.add(new LiteralStatement(customTemplateOptionsScript));
        }

        LOG.debug("Machine we are about to create in "+this+" will be customized with: "+
            statements);

        return new UserCreation(createdUserCreds, statements);  
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
        if (isWindows(node, setup)) {
            return registerWinRmMachineLocation(computeService, node, null, Optional.<HostAndPort>absent(), setup);
        } else {
            try {
                return registerJcloudsSshMachineLocation(computeService, node, Optional.<Template>absent(), null, Optional.<HostAndPort>absent(), setup);
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

    /**
     * Determines whether a machine may be rebinded to by comparing the given id, hostname and region
     * against the node's id, hostname, provider id and public addresses.
     */
    private static class RebindToMachinePredicate implements Predicate<ComputeMetadata> {

        final String rawId;
        final String rawHostname;
        final String rawRegion;

        public RebindToMachinePredicate(ConfigBag config) {
            rawId = (String) config.getStringKey("id");
            rawHostname = (String) config.getStringKey("hostname");
            rawRegion = (String) config.getStringKey("region");
        }

        @Override
        public boolean apply(ComputeMetadata input) {
            // ID exact match
            if (rawId != null) {
                // Second is AWS format
                if (rawId.equals(input.getId()) || rawRegion != null && (rawRegion + "/" + rawId).equals(input.getId())) {
                    return true;
                }
            }

            // else do node metadata lookup
            if (input instanceof NodeMetadata) {
                NodeMetadata node = NodeMetadata.class.cast(input);
                if (rawHostname != null && rawHostname.equalsIgnoreCase(node.getHostname()))
                    return true;
                if (rawHostname != null && node.getPublicAddresses().contains(rawHostname))
                    return true;
                if (rawId != null && rawId.equalsIgnoreCase(node.getHostname()))
                    return true;
                if (rawId != null && node.getPublicAddresses().contains(rawId))
                    return true;
                // don't do private IPs because they might be repeated
                if (rawId != null && rawId.equalsIgnoreCase(node.getProviderId()))
                    return true;
                if (rawHostname != null && rawHostname.equalsIgnoreCase(node.getProviderId()))
                    return true;
            }

            return false;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .omitNullValues()
                    .add("id", rawId)
                    .add("hostname", rawHostname)
                    .add("region", rawRegion)
                    .toString();
        }
    }

    // -------------- create the SshMachineLocation instance, and connect to it etc ------------------------

    /** @deprecated since 0.7.0 use {@link #registerJcloudsSshMachineLocation(ComputeService, NodeMetadata, LoginCredentials, Optional, ConfigBag)} */
    @Deprecated
    protected final JcloudsSshMachineLocation registerJcloudsSshMachineLocation(NodeMetadata node, String vmHostname, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) throws IOException {
        LOG.warn("Using deprecated registerJcloudsSshMachineLocation: now wants computeService passed", new Throwable("source of deprecated registerJcloudsSshMachineLocation invocation"));
        return registerJcloudsSshMachineLocation(null, node, Optional.<Template>absent(), null, sshHostAndPort, setup);
    }

    /**
     * @deprecated since 0.9.0; see {@link #registerJcloudsSshMachineLocation(ComputeService, NodeMetadata, Optional, LoginCredentials, Optional, ConfigBag)}.
     *             Marked as final to warn those trying to sub-class. 
     */
    @Deprecated
    protected final JcloudsSshMachineLocation registerJcloudsSshMachineLocation(ComputeService computeService, NodeMetadata node, LoginCredentials userCredentials, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) throws IOException {
        return registerJcloudsSshMachineLocation(computeService, node, Optional.<Template>absent(), userCredentials, sshHostAndPort, setup);
    }
    
    protected JcloudsSshMachineLocation registerJcloudsSshMachineLocation(ComputeService computeService, NodeMetadata node, Optional<Template> template, LoginCredentials userCredentials, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) throws IOException {
        if (userCredentials == null)
            userCredentials = node.getCredentials();

        String vmHostname = getPublicHostname(node, sshHostAndPort, userCredentials, setup);

        JcloudsSshMachineLocation machine = createJcloudsSshMachineLocation(computeService, node, template, vmHostname, sshHostAndPort, userCredentials, setup);
        registerJcloudsMachineLocation(node.getId(), machine);
        return machine;
    }

    @VisibleForTesting
    protected void registerJcloudsMachineLocation(String nodeId, JcloudsMachineLocation machine) {
        machine.setParent(this);
        vmInstanceIds.put(machine, nodeId);
    }

    /**
     * @deprecated since 0.9.0; see {@link #createJcloudsSshMachineLocation(ComputeService, NodeMetadata, Optional, String, Optional, LoginCredentials, ConfigBag)}.
     *             Marked as final to warn those trying to sub-class. 
     */
    @Deprecated
    protected final JcloudsSshMachineLocation createJcloudsSshMachineLocation(ComputeService computeService, NodeMetadata node, String vmHostname, Optional<HostAndPort> sshHostAndPort, LoginCredentials userCredentials, ConfigBag setup) throws IOException {
        return createJcloudsSshMachineLocation(computeService, node, Optional.<Template>absent(), vmHostname, sshHostAndPort, userCredentials, setup);
    }
    
    protected JcloudsSshMachineLocation createJcloudsSshMachineLocation(ComputeService computeService, NodeMetadata node, Optional<Template> template, String vmHostname, Optional<HostAndPort> sshHostAndPort, LoginCredentials userCredentials, ConfigBag setup) throws IOException {
        Map<?,?> sshConfig = extractSshConfig(setup, node);
        String nodeAvailabilityZone = extractAvailabilityZone(setup, node);
        String nodeRegion = extractRegion(setup, node);
        if (nodeRegion == null) {
            // e.g. rackspace doesn't have "region", so rackspace-uk is best we can say (but zone="LON")
            nodeRegion = extractProvider(setup, node);
        }

        String address = sshHostAndPort.isPresent() ? sshHostAndPort.get().getHostText() : vmHostname;
        try {
            Networking.getInetAddressWithFixedName(address);
            // fine, it resolves
        } catch (Exception e) {
            // occurs if an unresolvable hostname is given as vmHostname, and the machine only has private IP addresses but they are reachable
            // TODO cleanup use of getPublicHostname so its semantics are clearer, returning reachable hostname or ip, and
            // do this check/fix there instead of here!
            Exceptions.propagateIfFatal(e);
            LOG.debug("Could not resolve reported address '"+address+"' for "+vmHostname+" ("+setup.getDescription()+"/"+node+"), requesting reachable address");
            if (computeService==null) throw Exceptions.propagate(e);
            // this has sometimes already been done in waitForReachable (unless skipped) but easy enough to do again
            address = getFirstReachableAddress(node, setup);
        }

        if (LOG.isDebugEnabled())
            LOG.debug("creating JcloudsSshMachineLocation representation for {}@{} ({}/{}) for {}/{}",
                    new Object[] {
                            getUser(setup),
                            address,
                            Sanitizer.sanitize(sshConfig),
                            sshHostAndPort,
                            setup.getDescription(),
                            node
                    });

        if (isManaged()) {
            return getManagementContext().getLocationManager().createLocation(LocationSpec.create(JcloudsSshMachineLocation.class)
                    .configure(sshConfig)
                    .configure("displayName", vmHostname)
                    .configure("address", address)
                    .configure(JcloudsSshMachineLocation.SSH_PORT, sshHostAndPort.isPresent() ? sshHostAndPort.get().getPort() : node.getLoginPort())
                    .configure("user", userCredentials.getUser())
                    .configure(SshMachineLocation.PASSWORD.getName(), sshConfig.get(SshMachineLocation.PASSWORD.getName()) != null ?
                            sshConfig.get(SshMachineLocation.PASSWORD.getName()) :
                            userCredentials.getOptionalPassword().orNull())
                    .configure(SshMachineLocation.PRIVATE_KEY_DATA.getName(), sshConfig.get(SshMachineLocation.PRIVATE_KEY_DATA.getName()) != null ?
                            sshConfig.get(SshMachineLocation.PRIVATE_KEY_DATA.getName()) :
                            userCredentials.getOptionalPrivateKey().orNull())
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
                    .configureIfNotNull(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER)));
        } else {
            LOG.warn("Using deprecated JcloudsSshMachineLocation constructor because "+this+" is not managed");
            return new JcloudsSshMachineLocation(MutableMap.builder()
                    .putAll(sshConfig)
                    .put("displayName", vmHostname)
                    .put("address", address)
                    .put("port", sshHostAndPort.isPresent() ? sshHostAndPort.get().getPort() : node.getLoginPort())
                    .put("user", userCredentials.getUser())
                    .putIfNotNull(SshMachineLocation.PASSWORD.getName(), sshConfig.get(SshMachineLocation.PASSWORD.getName()) != null ?
                            SshMachineLocation.PASSWORD.getName() :
                            userCredentials.getOptionalPassword().orNull())
                    .putIfNotNull(SshMachineLocation.PRIVATE_KEY_DATA.getName(), sshConfig.get(SshMachineLocation.PRIVATE_KEY_DATA.getName()) != null ?
                            SshMachineLocation.PRIVATE_KEY_DATA.getName() :
                            userCredentials.getOptionalPrivateKey().orNull())
                    .put("callerContext", setup.get(CALLER_CONTEXT))
                    .putIfNotNull(CLOUD_AVAILABILITY_ZONE_ID.getName(), nodeAvailabilityZone)
                    .putIfNotNull(CLOUD_REGION_ID.getName(), nodeRegion)
                    .put(USE_PORT_FORWARDING, setup.get(USE_PORT_FORWARDING))
                    .put(PORT_FORWARDER, setup.get(PORT_FORWARDER))
                    .put(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER))
                    .build(),
                    this,
                    node);
        }
    }

    protected JcloudsWinRmMachineLocation registerWinRmMachineLocation(ComputeService computeService, NodeMetadata node, LoginCredentials initialCredentials, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) {
        if (initialCredentials==null)
            initialCredentials = node.getCredentials();

        String vmHostname = getPublicHostname(node, sshHostAndPort, setup);

        JcloudsWinRmMachineLocation machine = createWinRmMachineLocation(computeService, node, vmHostname, sshHostAndPort, setup);
        registerJcloudsMachineLocation(node.getId(), machine);
        return machine;
    }

    protected JcloudsWinRmMachineLocation createWinRmMachineLocation(ComputeService computeService, NodeMetadata node, String vmHostname, Optional<HostAndPort> sshHostAndPort, ConfigBag setup) {
        Map<?,?> winrmConfig = extractWinrmConfig(setup, node);
        String nodeAvailabilityZone = extractAvailabilityZone(setup, node);
        String nodeRegion = extractRegion(setup, node);
        if (nodeRegion == null) {
            // e.g. rackspace doesn't have "region", so rackspace-uk is best we can say (but zone="LON")
            nodeRegion = extractProvider(setup, node);
        }
        
        String address = sshHostAndPort.isPresent() ? sshHostAndPort.get().getHostText() : vmHostname;

        if (isManaged()) {
            return getManagementContext().getLocationManager().createLocation(LocationSpec.create(JcloudsWinRmMachineLocation.class)
                    .configure(winrmConfig)
                    .configure("jcloudsParent", this)
                    .configure("displayName", vmHostname)
                    .configure("address", address)
                    .configure(WinRmMachineLocation.WINRM_CONFIG_PORT, sshHostAndPort.isPresent() ? sshHostAndPort.get().getPort() : node.getLoginPort())
                    .configure("user", getUser(setup))
                    .configure(WinRmMachineLocation.USER, setup.get(USER))
                    .configure("node", node)
                    .configureIfNotNull(CLOUD_AVAILABILITY_ZONE_ID, nodeAvailabilityZone)
                    .configureIfNotNull(CLOUD_REGION_ID, nodeRegion)
                    .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT))
                    .configure(SshMachineLocation.DETECT_MACHINE_DETAILS, setup.get(SshMachineLocation.DETECT_MACHINE_DETAILS))
                    .configureIfNotNull(SshMachineLocation.SCRIPT_DIR, setup.get(SshMachineLocation.SCRIPT_DIR))
                    .configureIfNotNull(USE_PORT_FORWARDING, setup.get(USE_PORT_FORWARDING))
                    .configureIfNotNull(PORT_FORWARDER, setup.get(PORT_FORWARDER))
                    .configureIfNotNull(PORT_FORWARDING_MANAGER, setup.get(PORT_FORWARDING_MANAGER)));
        } else {
            throw new UnsupportedOperationException("Cannot create WinRmMachineLocation because " + this + " is not managed");
        }
    }

    // -------------- give back the machines------------------

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

        ConfigBag setup = config().getBag();
        for (JcloudsLocationCustomizer customizer : getCustomizers(setup)) {
            try {
                customizer.preRelease(machine);
            } catch (Exception e) {
                LOG.error("Problem invoking pre-release customizer "+customizer+" for machine "+machine+" in "+this+", instance id "+instanceId+
                    "; ignoring and continuing, "
                    + (tothrow==null ? "will throw subsequently" : "swallowing due to previous error")+": "+e, e);
                if (tothrow==null) tothrow = e;
            }
        }
        for (MachineLocationCustomizer customizer : getMachineCustomizers(setup)) {
            customizer.preRelease(machine);
        }

        try {
            // FIXME: Needs to release port forwarding for WinRmMachineLocations
            if (machine instanceof JcloudsMachineLocation) {
                releasePortForwarding((JcloudsMachineLocation)machine);
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

        for (JcloudsLocationCustomizer customizer : getCustomizers(setup)) {
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
     */
    protected LoginCredentials extractVmCredentials(ConfigBag setup, NodeMetadata node, LoginCredentials nodeCredentials) {
        String user = getUser(setup);
        OsCredential localCredentials = LocationConfigUtils.getOsCredential(setup).checkNoErrors();

        LOG.debug("Credentials extracted for {}: {}/{} with {}/{}", new Object[] { node,
            user, nodeCredentials.getUser(), localCredentials, nodeCredentials });

        if (Strings.isNonBlank(nodeCredentials.getUser())) {
            if (Strings.isBlank(user)) {
                setup.put(USER, user = nodeCredentials.getUser());
            } else if (ROOT_USERNAME.equals(user) && ROOT_ALIASES.contains(nodeCredentials.getUser())) {
                // deprecated, we used to default username to 'root'; now we leave null, then use autodetected credentials if no user specified
                LOG.warn("overriding username 'root' in favour of '"+nodeCredentials.getUser()+"' at {}; this behaviour may be removed in future", node);
                setup.put(USER, user = nodeCredentials.getUser());
            }

            String pkd = Strings.maybeNonBlank(localCredentials.getPrivateKeyData()).or(nodeCredentials.getOptionalPrivateKey().orNull());
            String pwd = Strings.maybeNonBlank(localCredentials.getPassword()).or(nodeCredentials.getOptionalPassword().orNull());
            if (Strings.isBlank(user) || (Strings.isBlank(pkd) && pwd==null)) {
                String missing = (user==null ? "user" : "credential");
                LOG.warn("Not able to determine "+missing+" for "+this+" at "+node+"; will likely fail subsequently");
                return null;
            } else {
                LoginCredentials.Builder resultBuilder = LoginCredentials.builder().user(user);
                if (pwd!=null && (Strings.isBlank(pkd) || localCredentials.isUsingPassword()))
                    resultBuilder.password(pwd);
                else // pkd guaranteed non-blank due to above
                    resultBuilder.privateKey(pkd);
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

            Predicate<? super HostAndPort> pollForFirstReachableHostAndPortPredicate;
            if (setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE) != null) {
                LOG.debug("Using pollForFirstReachableAddress.predicate supplied from config for location " + this + " "
                        + POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + " : " + setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE));
                pollForFirstReachableHostAndPortPredicate = setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE);
            } else {
                LOG.debug("Using pollForFirstReachableAddress.predicate.type supplied from config for location " + this + " " + POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE.getName()
                        + " : " + setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE));

                Class<? extends Predicate<? super HostAndPort>> predicateType = setup.get(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE);

                Map<String, Object> args = MutableMap.of();
                ConfigUtils.addUnprefixedConfigKeyInConfigBack(POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ".", setup, args);
                try {
                    pollForFirstReachableHostAndPortPredicate = predicateType.getConstructor(Map.class).newInstance(args);
                } catch (NoSuchMethodException|IllegalAccessException e) {
                    try {
                        pollForFirstReachableHostAndPortPredicate = predicateType.newInstance();
                    } catch (IllegalAccessException|InstantiationException newInstanceException) {
                        throw Exceptions.propagate("Instantiating " + predicateType + " failed.", newInstanceException);
                    }
                } catch (InvocationTargetException|InstantiationException e) {
                    throw Exceptions.propagate("Problem trying to instantiate " + predicateType + " with Map constructor.", e);
                }
            }

            try {
                result = JcloudsUtil.getFirstReachableAddress(node, timeout, pollForFirstReachableHostAndPortPredicate);
            } catch (Exception e) {
                throw Exceptions.propagate("Problem instantiating reachability checker for " + this
                        + " pollForFirstReachableHostAndPortPredicate: " + pollForFirstReachableHostAndPortPredicate, e);
            }
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

    protected LoginCredentials waitForWinRmAvailable(final ComputeService computeService, final NodeMetadata node, Optional<HostAndPort> hostAndPortOverride, ConfigBag setup) {
        return waitForWinRmAvailable(computeService, node, hostAndPortOverride, ImmutableList.of(node.getCredentials()), setup);
    }
    
    protected LoginCredentials waitForWinRmAvailable(final ComputeService computeService, final NodeMetadata node, Optional<HostAndPort> hostAndPortOverride, List<LoginCredentials> credentialsToTry, ConfigBag setup) {
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
        
        Set<String> users = Sets.newLinkedHashSet();
        for (LoginCredentials creds : credentialsToTry) {
            users.add(creds.getUser());
        }
        String user = (users.size() == 1) ? Iterables.getOnlyElement(users) : "{" + Joiner.on(",").join(users) + "}";
        String vmIp = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getHostText() : getFirstReachableAddress(node, setup);
        if (vmIp==null) LOG.warn("Unable to extract IP for "+node+" ("+setup.getDescription()+"): subsequent connection attempt will likely fail");
        int defaultWinRmPort = getConfig(WinRmMachineLocation.USE_HTTPS_WINRM) ? 5986 : 5985;
        int vmPort = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getPortOrDefault(defaultWinRmPort) : defaultWinRmPort;

        String connectionDetails = user + "@" + vmIp + ":" + vmPort;
        final HostAndPort hostAndPort = hostAndPortOverride.isPresent() ? hostAndPortOverride.get() : HostAndPort.fromParts(vmIp, vmPort);
        final AtomicReference<LoginCredentials> credsSuccessful = new AtomicReference<LoginCredentials>();

        // Don't use config that relates to the final user credentials (those have nothing to do
        // with the initial credentials of the VM returned by the cloud provider).
        // The createTemporaryWinRmMachineLocation deals with removing that.
        ConfigBag winrmProps = ConfigBag.newInstanceCopying(setup);

        final Map<WinRmMachineLocation, LoginCredentials> machinesToTry = Maps.newLinkedHashMap();
        for (LoginCredentials creds : credentialsToTry) {
            machinesToTry.put(createTemporaryWinRmMachineLocation(hostAndPort, creds, winrmProps), creds);
        }
        try {
            Callable<Boolean> checker = new Callable<Boolean>() {
                public Boolean call() {
                    for (Map.Entry<WinRmMachineLocation, LoginCredentials> entry : machinesToTry.entrySet()) {
                        final WinRmMachineLocation machine = entry.getKey();
                        WinRmToolResponse response = machine.executeCommand(
                                ImmutableMap.of(WinRmTool.PROP_EXEC_TRIES.getName(), 1),
                                ImmutableList.of("echo testing"));
                        boolean success = (response.getStatusCode() == 0);
                        if (success) {
                            credsSuccessful.set(entry.getValue());

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
                    }
                    return false;
                }};

            waitForReachable(checker, connectionDetails, credentialsToTry, setup, timeout);
        } finally {
            for (WinRmMachineLocation machine : machinesToTry.keySet()) {
                if (getManagementContext().getLocationManager().isManaged(machine)) {
                    // get benign but unpleasant warnings if we unmanage something already unmanaged
                    getManagementContext().getLocationManager().unmanage(machine);
                }
            }
        }

        return credsSuccessful.get();
    }

    protected LoginCredentials waitForSshable(final ComputeService computeService, final NodeMetadata node, Optional<HostAndPort> hostAndPortOverride, ConfigBag setup) {
        LoginCredentials nodeCreds = node.getCredentials();
        String nodeUser = nodeCreds.getUser();
        String loginUserOverride = setup.get(LOGIN_USER);
        Set<String> users = MutableSet.of();

        if (Strings.isNonBlank(nodeUser)) {
            users.add(nodeUser);
        }

        if (Strings.isNonBlank(loginUserOverride)) {
            users.add(loginUserOverride);
        }

        // See https://issues.apache.org/jira/browse/BROOKLYN-186
        // Handle where jclouds gives us the wrong login user (!) and both a password + ssh key.
        // Try all the permutations to find the one that works.
        List<LoginCredentials> credentialsToTry = Lists.newArrayList();
        for (String user : users) {
            if (nodeCreds.getOptionalPassword().isPresent() && nodeCreds.getOptionalPrivateKey().isPresent()) {
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).noPassword().user(user).build());
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).noPrivateKey().user(user).build());
            } else {
                credentialsToTry.add(LoginCredentials.builder(nodeCreds).user(user).build());
            }
        }
        
        return waitForSshable(computeService, node, hostAndPortOverride, credentialsToTry, setup);
    }
    
    protected LoginCredentials waitForSshable(final ComputeService computeService, final NodeMetadata node, Optional<HostAndPort> hostAndPortOverride, List<LoginCredentials> credentialsToTry, ConfigBag setup) {
        String waitForSshable = setup.get(WAIT_FOR_SSHABLE);
        checkArgument(!"false".equalsIgnoreCase(waitForSshable), "waitForReachable called despite waitForSshable=%s for %s", waitForSshable, node);
        checkArgument(credentialsToTry.size() > 0, "waitForReachable called without credentials for %s", node);

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
        String vmIp = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getHostText() : getFirstReachableAddress(node, setup);
        if (vmIp==null) LOG.warn("Unable to extract IP for "+node+" ("+setup.getDescription()+"): subsequent connection attempt will likely fail");
        int vmPort = hostAndPortOverride.isPresent() ? hostAndPortOverride.get().getPortOrDefault(22) : getLoginPortOrDefault(node, 22);

        String connectionDetails = user + "@" + vmIp + ":" + vmPort;
        final HostAndPort hostAndPort = hostAndPortOverride.isPresent() ? hostAndPortOverride.get() : HostAndPort.fromParts(vmIp, vmPort);
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
        try {
            Callable<Boolean> checker = new Callable<Boolean>() {
                public Boolean call() {
                    for (Map.Entry<SshMachineLocation, LoginCredentials> entry : machinesToTry.entrySet()) {
                        SshMachineLocation machine = entry.getKey();
                        int exitstatus = machine.execScript(
                                ImmutableMap.of(
                                        SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), Duration.THIRTY_SECONDS.toMilliseconds(),
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

    protected void waitForReachable(Callable<Boolean> checker, String hostAndPort, List<LoginCredentials> credentialsToLog, ConfigBag setup, Duration timeout) {
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
                            setup.getDescription(), timeout,
                            hostAndPort,
                            credentialsToLog.size(),
                            Strings.s(credentialsToLog.size()),
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
                    +hostAndPort+" ("+setup.getDescription()+") after waiting "
                    +Time.makeTimeStringRounded(timeout), reachable.getError());
        }

        LOG.debug("VM {}: connection succeeded after {} on {}",new Object[] {
                setup.getDescription(), Time.makeTimeStringRounded(stopwatch),
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

        return getPublicHostnameGeneric(node, setup);
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

        return getPrivateHostnameGeneric(node, setup);
    }

    private String getPublicHostnameGeneric(NodeMetadata node, @Nullable ConfigBag setup) {
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
            return node.getPublicAddresses().iterator().next();
        } else if (groovyTruth(node.getPrivateAddresses())) {
            return node.getPrivateAddresses().iterator().next();
        } else {
            return null;
        }
    }

    private String getPrivateHostnameGeneric(NodeMetadata node, @Nullable ConfigBag setup) {
        //prefer the private address to the hostname because hostname is sometimes wrong/abbreviated
        //(see that javadoc; also e.g. on rackspace/cloudstack, the hostname is not registered with any DNS).
        //Don't return local-only address (e.g. never 127.0.0.1)
        if (groovyTruth(node.getPrivateAddresses())) {
            for (String p : node.getPrivateAddresses()) {
                if (Networking.isLocalOnly(p)) continue;
                return p;
            }
        }
        if (groovyTruth(node.getPublicAddresses())) {
            return node.getPublicAddresses().iterator().next();
        } else if (groovyTruth(node.getHostname())) {
            return node.getHostname();
        } else {
            return null;
        }
    }

    private Maybe<String> getHostnameAws(NodeMetadata node, Optional<HostAndPort> sshHostAndPort, Supplier<? extends LoginCredentials> userCredentials, ConfigBag setup) {
        HostAndPort inferredHostAndPort = null;
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
                LOG.warn("Cannpy query aws-ec2 Windows instance "+node.getId()+"@"+node.getLocation()+" over ssh for its hostname; falling back to jclouds metadata for address");
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

    private String getHostnameAws(HostAndPort hostAndPort, LoginCredentials userCredentials, ConfigBag setup) {
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

    // ------------ static converters (could go to a new file) ------------------

    public static File asFile(Object o) {
        if (o instanceof File) return (File)o;
        if (o == null) return null;
        return new File(o.toString());
    }

    public static String fileAsString(Object o) {
        if (o instanceof String) return (String)o;
        if (o instanceof File) return ((File)o).getAbsolutePath();
        if (o==null) return null;
        return o.toString();
    }


    protected static double toDouble(Object v) {
        if (v instanceof Number) {
            return ((Number)v).doubleValue();
        } else {
            throw new IllegalArgumentException("Invalid type for double: "+v+" of type "+v.getClass());
        }
    }

    protected static String[] toStringArray(Object v) {
        return toListOfStrings(v).toArray(new String[0]);
    }

    protected static List<String> toListOfStrings(Object v) {
        List<String> result = Lists.newArrayList();
        if (v instanceof Iterable) {
            for (Object o : (Iterable<?>)v) {
                result.add(o.toString());
            }
        } else if (v instanceof Object[]) {
            for (int i = 0; i < ((Object[])v).length; i++) {
                result.add(((Object[])v)[i].toString());
            }
        } else if (v instanceof String) {
            result.add((String) v);
        } else {
            throw new IllegalArgumentException("Invalid type for List<String>: "+v+" of type "+v.getClass());
        }
        return result;
    }

    protected static byte[] toByteArray(Object v) {
        if (v instanceof byte[]) {
            return (byte[]) v;
        } else if (v instanceof CharSequence) {
            return v.toString().getBytes();
        } else {
            throw new IllegalArgumentException("Invalid type for byte[]: "+v+" of type "+v.getClass());
        }
    }

    @VisibleForTesting
    static int[] toIntPortArray(Object v) {
        PortRange portRange = PortRanges.fromIterable(Collections.singletonList(v));
        int[] portArray = ArrayUtils.toPrimitive(Iterables.toArray(portRange, Integer.class));

        return portArray;
    }

    // Handles GString
    protected static Map<String,String> toMapStringString(Object v) {
        if (v instanceof Map<?,?>) {
            Map<String,String> result = Maps.newLinkedHashMap();
            for (Map.Entry<?,?> entry : ((Map<?,?>)v).entrySet()) {
                String key = ((CharSequence)entry.getKey()).toString();
                String value = ((CharSequence)entry.getValue()).toString();
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

    private List<String> createIptablesRulesForNetworkInterface(Iterable<Integer> ports) {
       List<String> iptablesRules = Lists.newArrayList();
       for (Integer port : ports) {
          iptablesRules.add(IptablesCommands.insertIptablesRule(Chain.INPUT, Protocol.TCP, port, Policy.ACCEPT));
       }
       return iptablesRules;
    }

    @Override
    public PersistenceObjectStore newPersistenceObjectStore(String container) {
        return new JcloudsBlobStoreBasedObjectStore(this, container);
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
