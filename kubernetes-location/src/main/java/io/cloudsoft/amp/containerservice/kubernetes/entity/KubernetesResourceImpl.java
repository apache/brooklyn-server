package io.cloudsoft.amp.containerservice.kubernetes.entity;

import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessImpl;

public class KubernetesResourceImpl extends EmptySoftwareProcessImpl implements KubernetesResource {

    @Override
    public void init() {
       super.init();

      config().set(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true);
      config().set(PROVISIONING_PROPERTIES.subKey("useJcloudsSshInit"), false);
      config().set(PROVISIONING_PROPERTIES.subKey("waitForSshable"), false);
      config().set(EmptySoftwareProcessImpl.USE_SSH_MONITORING, false);
    }
}
