package io.cloudsoft.amp.containerservice.kubernetes.entity;

import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainerImpl;

public class KubernetesPodImpl extends DockerContainerImpl implements KubernetesPod {

    @Override
    public void init() {
        super.init();

        if (config().get(IMAGE_NAME) == null && config().get(INBOUND_TCP_PORTS) == null) {
            config().set(CHILDREN_STARTABLE_MODE, ChildStartableMode.BACKGROUND);
        }
    }

}
