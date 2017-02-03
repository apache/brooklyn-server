package io.cloudsoft.amp.containerservice.kubernetes.entity;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static org.apache.brooklyn.core.location.LocationPredicates.configEqualTo;

import java.util.Collection;

import org.apache.brooklyn.api.location.Location;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainerImpl;
import io.cloudsoft.amp.containerservice.kubernetes.location.machine.KubernetesEmptyMachineLocation;
import io.cloudsoft.amp.containerservice.kubernetes.location.machine.KubernetesMachineLocation;

public class KubernetesPodImpl extends DockerContainerImpl implements KubernetesPod {

    @Override
    public void init() {
        super.init();

        if (config().get(IMAGE_NAME) == null && config().get(INBOUND_TCP_PORTS) == null) {
            config().set(CHILDREN_STARTABLE_MODE, ChildStartableMode.BACKGROUND_LATE);
        }
    }

    // FIXME I hate myself...
    @Override
    public Collection<Location> getLocations() {
        // TODO Remove this filtering once backwards compatible KubernetesPod usage is deprecated
        return ImmutableSet.copyOf(Iterables.filter(super.getLocations(),
                not(and(instanceOf(KubernetesEmptyMachineLocation.class),
                        configEqualTo(KubernetesMachineLocation.KUBERNETES_RESOURCE_TYPE, KubernetesPod.EMPTY)))));
    }

}
