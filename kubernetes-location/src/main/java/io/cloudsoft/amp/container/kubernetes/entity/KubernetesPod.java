package io.cloudsoft.amp.container.kubernetes.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.entity.stock.BasicStartable;

@ImplementedBy(KubernetesPodImpl.class)
public interface KubernetesPod extends BasicStartable {
}
