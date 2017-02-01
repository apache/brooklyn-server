package io.cloudsoft.amp.containerservice.kubernetes.location;

import org.apache.brooklyn.location.ssh.SshMachineLocation;

/**
 * A {@link MachineLocation} represemnting a Kubernetes resource that allows SSH access.
 *
 * @see {@link KubernetesSshMachineLocation}
 */
public class KubernetesSshMachineLocation extends SshMachineLocation implements KubernetesMachineLocation {

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
