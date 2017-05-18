package io.cloudsoft.amp.containerservice.kubernetes.location.machine;

import java.net.InetAddress;
import java.util.Set;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.net.Networking;

import com.google.common.collect.ImmutableSet;

/**
 * A {@link MachineLocation} represemnting a Kubernetes resource that does not support SSH access.
 *
 * @see {@link KubernetesSshMachineLocation}
 */
public class KubernetesEmptyMachineLocation extends SshMachineLocation implements KubernetesMachineLocation {

    @Override
    public String getHostname() {
        return getResourceName();
    }

    @Override
    public Set<String> getPublicAddresses() {
        return ImmutableSet.of("0.0.0.0");
    }

    @Override
    public Set<String> getPrivateAddresses() {
        return ImmutableSet.of("0.0.0.0");
    }

    @Override
    public InetAddress getAddress() {
        return Networking.getInetAddressWithFixedName("0.0.0.0");
    }

    @Override
    public OsDetails getOsDetails() {
        return null;
        // throw new UnsupportedOperationException("No OS details for empty KubernetesMachineLocation");
    }

    @Override
    public MachineDetails getMachineDetails() {
        return null;
        // throw new UnsupportedOperationException("No machine details for empty KubernetesMachineLocation");
    }

    @Override
    public String getResourceName() {
        return config().get(KUBERNETES_RESOURCE_NAME);
    }

    @Override
    public String getResourceType() {
        return config().get(KUBERNETES_RESOURCE_TYPE);
    }

    @Override
    public String getNamespace() {
        return config().get(KUBERNETES_NAMESPACE);
    }

}
