package io.cloudsoft.amp.containerservice.kubernetes.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.entity.stock.BasicStartable;

@ImplementedBy(KubernetesPodImpl.class)
public interface KubernetesPod extends BasicStartable {
}
