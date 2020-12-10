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

import org.apache.brooklyn.api.location.Location;
import static org.apache.brooklyn.location.jclouds.api.JcloudsLocationConfigPublic.USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.location.HardwareDetails;
import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.core.location.BasicHardwareDetails;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.location.BasicOsDetails;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.location.LocationConfigUtils.OsCredential;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

public class JcloudsSshMachineLocation extends SshMachineLocation implements JcloudsMachineLocation {
    
    private static final Logger LOG = LoggerFactory.getLogger(JcloudsSshMachineLocation.class);

    @SetFromFlag
    JcloudsLocation jcloudsParent;
    
    /**
     * @deprecated since 0.9.0; the node should not be persisted.
     */
    @SetFromFlag
    @Deprecated
    NodeMetadata node;
    
    /**
     * @deprecated since 0.9.0; the template should not be persisted.
     */
    @SetFromFlag
    @Deprecated
    Template template;
    
    @SetFromFlag
    String nodeId;

    @SetFromFlag
    String imageId;

    @SetFromFlag
    Set<String> privateAddresses;
    
    @SetFromFlag
    Set<String> publicAddresses;

    @SetFromFlag
    String hostname;

    // Populated lazily, on first call to getSubnetHostname()
    @SetFromFlag
    String privateHostname;

    /**
     * Historically, "node" and "template" were persisted. However that is a very bad idea!
     * It means we pull in lots of jclouds classes into the persisted state. We are at an  
     * intermediate stage, where we want to handle rebinding to old state that has "node"
     * and new state that should not. We therefore leave in the {@code @SetFromFlag} on node
     * so that we read it back, but we ensure the value is null when we write it out!
     *
     * Note that fields in locations behave differently from those in entities. Locations will
     * update the persisted state with the latest values of fields and will skip transient
     * properties. Entities don't read back the field values.
     * 
     * TODO We will rename these to get rid of the ugly underscore when the old node/template 
     * fields are deleted.
     * TODO Should we change callers to pass all the bits of node & template we are interested
     * in instead of the objects themselves so we don't have to clear them here?
     */
    private transient Optional<NodeMetadata> _node;

    private transient Optional<Template> _template;

    private transient Optional<Image> _image;

    public JcloudsSshMachineLocation() {
    }
    
    /**
     * @deprecated since 0.6; use LocationSpec (which calls no-arg constructor)
     */
    @Deprecated
    public JcloudsSshMachineLocation(Map<?,?> flags, JcloudsLocation jcloudsParent, NodeMetadata node) {
        super(flags);
        this.jcloudsParent = jcloudsParent;
        
        init();
    }

    @Override
    public void init() {
        if (jcloudsParent != null) {
            super.init();
        } else {
            // TODO Need to fix the rebind-detection, and not call init() on rebind.
            // This will all change when locations become entities.
            // Note that the happy path for rebind will go through the above case!
            if (LOG.isDebugEnabled()) LOG.debug("Not doing init() of {} because parent not set; presuming rebinding", this);
        }
        clearDeprecatedProperties();
    }

    @Override
    public void rebind() {
        super.rebind();
        
        if (jcloudsParent == null) {
            // can be null on rebind, if location has been "orphaned" somehow
            LOG.warn("Location {} does not have parent; cannot retrieve jclouds compute-service; "
                    + "later operations may fail (continuing)", this);
        }
        clearDeprecatedProperties();
    }

    protected void clearDeprecatedProperties() {
        if (node != null) {
            setNode(node);
        }
        if (template != null) {
            setTemplate(template);
        }
    }
    
    @Override
    public String toVerboseString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("id", getId()).add("name", getDisplayName())
                .add("user", getUser()).add("address", getAddress()).add("port", getConfig(SSH_PORT))
                .add("node", _node)
                .add("nodeId", getJcloudsId())
                .add("imageId", getImageId())
                .add("privateAddresses", getPrivateAddresses())
                .add("publicAddresses", getPublicAddresses())
                .add("parentLocation", getParent())
                .add("osDetails", getOptionalOsDetails())
                .toString();
    }

    protected void setNode(NodeMetadata node) {
        this.node = null;
        config().removeKey("node");
        nodeId = node.getId();
        imageId = node.getImageId();
        publicAddresses = node.getPublicAddresses();
        _node = Optional.of(node);

        Boolean useMachinePublicAddressAsPrivateAddress = config().get(USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS);
        if(useMachinePublicAddressAsPrivateAddress) {
            LOG.debug("Overriding private address ["+node.getPrivateAddresses()+"] as public address ["+node.getPublicAddresses()+"] as config "+ USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS +" is set to true");
            privateAddresses = node.getPublicAddresses();
        } else {
            privateAddresses = node.getPrivateAddresses();
        }
    }

    protected void setTemplate(Template template) {
        this.template = null;
        config().removeKey("template");
        _template = Optional.of(template);
        _image = Optional.fromNullable(template.getImage());
    }

    protected ComputeService getComputeServiceOrNull() {
        JcloudsLocation parent = getParent();
        return (parent != null) ? parent.getComputeService() : null;
    }
    
    @Override
    public Optional<NodeMetadata> getOptionalNode() {
      if (_node == null) {
          try {
              ComputeService computeService = getComputeServiceOrNull();
              if (computeService == null) {
                  if (LOG.isDebugEnabled()) LOG.debug("Cannot get node for {}, because cannot get compute-service from parent {}", this, getParent());
                  _node = Optional.absent();
              } else {
                  _node = Optional.fromNullable(computeService.getNodeMetadata(nodeId));
              }
          } catch (Exception e) {
              Exceptions.propagateIfFatal(e);
              if (LOG.isDebugEnabled()) LOG.debug("Problem getting node-metadata for " + this + ", node id " + nodeId + " (continuing)", e);
              _node = Optional.absent();
          }
      }
      return _node;
    }

    protected Optional<Image> getOptionalImage() {
      if (_image == null) {
          if (imageId == null) {
              _image = Optional.absent(); // can happen with JcloudsLocation.resumeMachine() usage
          } else {
              try {
                  ComputeService computeService = getComputeServiceOrNull();
                  if (computeService == null) {
                      if (LOG.isDebugEnabled()) LOG.debug("Cannot get image (with id {}) for {}, because cannot get compute-service from parent {}", new Object[] {imageId, this, getParent()});
                      _image = Optional.absent();
                  } else {
                      _image = Optional.fromNullable(computeService.getImage(imageId));
                  }
              } catch (Exception e) {
                  Exceptions.propagateIfFatal(e);
                  if (LOG.isDebugEnabled()) LOG.debug("Problem getting image for " + this + ", image id " + imageId + " (continuing)", e);
                  _image = Optional.absent();
              }
          }
      }
      return _image;
    }

    /**
     * @since 0.9.0
     * @deprecated since 0.9.0 (only added as aid until the deprecated {@link #getTemplate()} is deleted)
     */
    @Deprecated
    protected Optional<Template> getOptionalTemplate() {
        if (_template == null) {
            _template = Optional.absent();
        }
        return _template;
    }

    /**
     * @deprecated since 0.9.0
     */
    @Override
    @Deprecated
    public NodeMetadata getNode() {
        Optional<NodeMetadata> result = getOptionalNode();
        if (result.isPresent()) {
            return result.get();
        } else {
            throw new IllegalStateException("Node "+nodeId+" not present in "+getParent());
        }
    }
    
    @VisibleForTesting
    Optional<NodeMetadata> peekNode() {
        return _node;
    }
    
    @Override
    public JcloudsLocation getParent() {
        return jcloudsParent;
    }

    @Override
    public void setParent(Location newParent, boolean updateChildListParents) {
        if (newParent==null || newParent instanceof JcloudsLocation) {
            // used to clear parent when removing from parent, to prevent releasing it
            jcloudsParent = (JcloudsLocation) newParent;
        }
        super.setParent(newParent, updateChildListParents);
    }

    @Override
    public String getHostname() {
        // Changed behaviour in Brooklyn 0.9.0. Previously it just did node.getHostname(), which
        // was wrong on some clouds (e.g. vcloud-director, where VMs are often given a random 
        // hostname that does not resolve on the VM and is not in any DNS).
        // Now delegates to jcloudsParent.getPublicHostname(node).
        if (hostname == null) {
            Optional<NodeMetadata> node = getOptionalNode();
            if (node.isPresent()) {
                HostAndPort sshHostAndPort = getSshHostAndPort();
                Supplier<LoginCredentials> creds = getLoginCredentialsSupplier();
                hostname = jcloudsParent.getPublicHostname(node.get(), Optional.of(sshHostAndPort), creds, config().getBag());
                requestPersist();

            } else {
                // Fallback: impl taken (mostly) from jcloudsParent.getPublicHostnameGeneric(NodeMetadata, ConfigBag).
                // But we won't have a node object (e.g. after rebind, and VM has been terminated).
                // We also resort to address.getHostAddress as final fallback.
                if (groovyTruth(getPublicAddresses())) {
                    hostname = getPublicAddresses().iterator().next();
                } else if (groovyTruth(getPrivateAddresses())) {
                    hostname = getPrivateAddresses().iterator().next();
                } else {
                    hostname = getAddress().getHostAddress();
                }
            }
            LOG.debug("Resolved hostname {} for {}", hostname, this);
            requestPersist();
        }
        return hostname;
    }
    
    /** In clouds like AWS, the public hostname is the only way to ensure VMs in different zones can access each other. */
    @Override
    public Set<String> getPublicAddresses() {
        return (publicAddresses == null) ? ImmutableSet.<String>of() : publicAddresses;
    }
    
    @Override
    public Set<String> getPrivateAddresses() {
        return (privateAddresses == null) ? ImmutableSet.<String>of() : privateAddresses;
    }

    @Override
    public String getSubnetHostname() {
        if (privateHostname == null) {
            Optional<NodeMetadata> node = getOptionalNode();
            if (node.isPresent()) {
                // Prefer jcloudsLocation.getPrivateHostname(): it handles AWS hostname in a special way, 
                // by querying AWS for the hostname that resolves both inside and outside of the region.
                // If we can't get the node (i.e. the cloud provider doesn't know that id, because it has
                // been terminated), then we don't care as much about getting the right id!
                HostAndPort sshHostAndPort = getSshHostAndPort();
                Supplier<LoginCredentials> creds = getLoginCredentialsSupplier();
                privateHostname = jcloudsParent.getPrivateHostname(node.get(), Optional.of(sshHostAndPort), creds, config().getBag());

            } else {
                // Fallback: impl taken from jcloudsParent.getPrivateHostnameGeneric(NodeMetadata, ConfigBag).
                // But we won't have a node object (e.g. after rebind, and VM has been terminated).
                //prefer the private address to the hostname because hostname is sometimes wrong/abbreviated
                //(see that javadoc; also e.g. on rackspace/cloudstack, the hostname is not registered with any DNS).
                //Don't return local-only address (e.g. never 127.0.0.1)
                for (String p : getPrivateAddresses()) {
                    if (Networking.isLocalOnly(p)) continue;
                    privateHostname = p;
                    break;
                }
                if (Strings.isBlank(privateHostname) && groovyTruth(getPublicAddresses())) {
                    privateHostname = getPublicAddresses().iterator().next();
                } else if (Strings.isBlank(privateHostname)) {
                    privateHostname = getHostname();
                }
            }
            requestPersist();
            LOG.debug("Resolved subnet hostname {} for {}", privateHostname, this);
        }
        
        return privateHostname;
    }

    @Override
    public String getSubnetIp() {
        // Previous to Brooklyn 0.9.0, this could return the hostname or would try to do
        // jcloudsParent.getPublicHostname, and return the resolved IP. That was clearly 
        // not a "subnet ip"!
        Optional<String> privateAddress = getPrivateAddress();
        if (privateAddress.isPresent()) {
            return privateAddress.get();
        }
        if (groovyTruth(getPublicAddresses())) {
            return getPublicAddresses().iterator().next();
        }
        return null;
    }

    protected Optional<String> getPrivateAddress() {
        for (String p : getPrivateAddresses()) {
            // disallow local only addresses
            if (!Networking.isLocalOnly(p)) {
                // other things may be public or private, but either way, return it
                return Optional.of(p);
            }
        }
        return Optional.absent();
    }
    
    @Override
    public String getJcloudsId() {
        return nodeId;
    }
    
    protected String getImageId() {
        return imageId;
    }

    /**
     * Retrieves the password for this VM, if one exists. The behaviour/implementation is different for different clouds.
     * e.g. on Rackspace, the password for a windows VM is available immediately; on AWS-EC2, for a Windows VM you need 
     * to poll repeatedly until the password is available which can take up to 15 minutes.
     * 
     * @deprecated since 0.9.0; use the machine to execute commands, so no need to extract the password
     */
    @Deprecated
    public String waitForPassword() {
        Optional<NodeMetadata> node = getOptionalNode();
        if (node.isPresent()) {
            // TODO Hacky; don't want aws specific stuff here but what to do?!
            if (jcloudsParent.getProvider().equals("aws-ec2")) {
                try {
                    return JcloudsUtil.waitForPasswordOnAws(jcloudsParent.getComputeService(), node.get(), 15, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    throw Throwables.propagate(e);
                }
            } else {
                LoginCredentials credentials = node.get().getCredentials();
                return (credentials != null) ? credentials.getOptionalPassword().orNull() : null;
            }
        } else {
            throw new IllegalStateException("Node "+nodeId+" not present in "+getParent());
        }
    }

    private Supplier<LoginCredentials> getLoginCredentialsSupplier() {
        return new Supplier<LoginCredentials>() {
            @Override public LoginCredentials get() {
                return getLoginCredentials();
            }
        };
    }
    
    private LoginCredentials getLoginCredentials() {
        OsCredential creds = LocationConfigUtils.getOsCredential(new ResolvingConfigBag(getManagementContext(), config().getBag()));
        
        return LoginCredentials.builder()
                .user(getUser())
                .privateKey(creds.hasKey() ? creds.getPrivateKeyData() : null)
                .password(creds.hasPassword() ? creds.getPassword() : null)
                .build();
    }

    /**
     * Returns the known OsDetails, without any attempt to retrieve them if not known.
     */
    protected Optional<OsDetails> getOptionalOsDetails() {
        Optional<MachineDetails> machineDetails = getOptionalMachineDetails();
        OsDetails result = machineDetails.isPresent() ? machineDetails.get().getOsDetails() : null;
        return Optional.fromNullable(result);
    }

    protected Optional<OperatingSystem> getOptionalOperatingSystem() {
        Optional<NodeMetadata> node = getOptionalNode();
        
        OperatingSystem os = node.isPresent() ? node.get().getOperatingSystem() : null;
        if (os == null) {
            // some nodes (eg cloudstack, gce) might not get OS available on the node,
            // so also try taking it from the image if available
            Optional<Image> image = getOptionalImage();
            if (image.isPresent()) {
                os = image.get().getOperatingSystem();
            }
        }
        return Optional.fromNullable(os);
    }

    protected Optional<Hardware> getOptionalHardware() {
        Optional<NodeMetadata> node = getOptionalNode();
        return Optional.fromNullable(node.isPresent() ? node.get().getHardware() : null);
    }
    
    @Override
    protected MachineDetails detectMachineDetails() {
        Optional<String> name = Optional.absent();
        Optional<String> version = Optional.absent();
        Optional<String> architecture = Optional.absent();
        Optional<OperatingSystem> os = getOptionalOperatingSystem();
        Optional<Hardware> hardware = getOptionalHardware();
        
        if (os.isPresent()) {
            // Note using family rather than name. Name is often unset.
            // Using is64Bit rather then getArch because getArch often returns "paravirtual"
            OsFamily family = os.get().getFamily();
            String versionRaw = os.get().getVersion();
            boolean is64Bit = os.get().is64Bit();
            name = Optional.fromNullable(family != null && !OsFamily.UNRECOGNIZED.equals(family) ? family.toString() : null);
            version = Optional.fromNullable(Strings.isNonBlank(versionRaw) ? versionRaw : null);
            architecture = Optional.fromNullable(is64Bit ? BasicOsDetails.OsArchs.X_86_64 : BasicOsDetails.OsArchs.I386);
        }

        Optional<Integer> ram = hardware.isPresent() ? Optional.fromNullable(hardware.get().getRam()) : Optional.<Integer>absent();
        Optional<Integer> cpus = hardware.isPresent() ? Optional.fromNullable(hardware.get().getProcessors() != null ? hardware.get().getProcessors().size() : null) : Optional.<Integer>absent();

        // Skip superclass' SSH to machine if all data is present, otherwise defer to super
        if (name.isPresent() && version.isPresent() && architecture.isPresent() && ram.isPresent() && cpus.isPresent()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Gathered machine details from Jclouds, skipping SSH test on {}", this);
            }
            OsDetails osD = new BasicOsDetails(name.get(), architecture.get(), version.get());
            HardwareDetails hwD = new BasicHardwareDetails(cpus.get(), ram.get());
            return new BasicMachineDetails(hwD, osD);
        } else if (!isManaged() || "false".equalsIgnoreCase(getConfig(JcloudsLocation.WAIT_FOR_SSHABLE))) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Machine details for {} missing from Jclouds, but skipping SSH test because {}. name={}, version={}, " +
                        "arch={}, ram={}, #cpus={}",
                        new Object[]{this, (isManaged() ? "waitForSshable=false" : "unmanaged"), name, version, architecture, ram, cpus});
            }
            OsDetails osD = new BasicOsDetails(name.orNull(), architecture.orNull(), version.orNull());
            HardwareDetails hwD = new BasicHardwareDetails(cpus.orNull(), ram.orNull());
            return new BasicMachineDetails(hwD, osD);
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Machine details for {} missing from Jclouds, using SSH test instead. name={}, version={}, " +
                                "arch={}, ram={}, #cpus={}",
                        new Object[]{this, name, version, architecture, ram, cpus});
            }
            return super.detectMachineDetails();
        }
    }

    @Override
    public Map<String, String> toMetadataRecord() {
        JcloudsLocation parent = getParent();
        Optional<NodeMetadata> node = getOptionalNode();
        Optional<Hardware> hardware = getOptionalHardware();
        List<? extends Processor> processors = hardware.isPresent() ? hardware.get().getProcessors() : null;
        
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(super.toMetadataRecord());
        putIfNotNull(builder, "provider", (parent != null) ? parent.getProvider() : null);
        putIfNotNull(builder, "account", (parent != null) ? parent.getIdentity() : null);
        putIfNotNull(builder, "region", (parent != null) ? parent.getRegion() : null);
        putIfNotNull(builder, "endpoint", (parent != null) ? parent.getEndpoint() : null);
        putIfNotNull(builder, "serverId", getJcloudsId());
        
        putIfNotNull(builder, "hostname", getHostname());
        putIfNotNull(builder, "address", getAddress()!=null ? getAddress().getHostAddress() : null);
        putIfNotNull(builder, "subnetHostname", getSubnetHostname());
        putIfNotNull(builder, "subnetAddress", getSubnetIp());
        
        putIfNotNull(builder, "imageId", getImageId());
        putIfNotNull(builder, "instanceTypeName", (hardware.isPresent() ? hardware.get().getName() : null));
        putIfNotNull(builder, "instanceTypeId", (hardware.isPresent() ? hardware.get().getProviderId() : null));
        putIfNotNull(builder, "ram", "" + (hardware.isPresent() ? hardware.get().getRam() : null));
        putIfNotNull(builder, "cpus", "" + (processors != null ? processors.size() : null));
        
        try {
            OsDetails osDetails = getOsDetails();
            putIfNotNull(builder, "osName", osDetails.getName());
            putIfNotNull(builder, "osArch", osDetails.getArch());
            putIfNotNull(builder, "is64bit", osDetails.is64bit() ? "true" : "false");
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.warn("Unable to get OS Details for "+node+"; continuing", e);
        }
        
        return builder.build();
    }
    
    private void putIfNotNull(ImmutableMap.Builder<String, String> builder, String key, @Nullable String value) {
        if (value != null) builder.put(key, value);
    }

}
