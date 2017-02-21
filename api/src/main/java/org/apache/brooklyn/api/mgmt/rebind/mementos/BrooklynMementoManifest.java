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
package org.apache.brooklyn.api.mgmt.rebind.mementos;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.objs.Identifiable;

/**
 * Represents a manifest of the entities etc in the overall memento.
 * 
 * @author aled
 */
public interface BrooklynMementoManifest extends Serializable {
    public interface EntityMementoManifest extends Identifiable{
        @Override
        String getId();
        String getType();
        String getParent();

        /**
         * deprecated since 0.11.0, use {@link #getCatalogItemHierarchy()} instead
         * @return
         */
        @Deprecated String getCatalogItemId();

        List<String> getCatalogItemHierarchy();
    }

    String getPlaneId();

    Map<String, EntityMementoManifest> getEntityIdToManifest();

    Map<String, String> getLocationIdToType();

    Map<String, String> getPolicyIdToType();

    Map<String, String> getEnricherIdToType();

    Map<String, String> getFeedIdToType();
    
    CatalogItemMemento getCatalogItemMemento(String id);

    Collection<String> getCatalogItemIds();

    Map<String, CatalogItemMemento> getCatalogItemMementos();

    boolean isEmpty();
    
}
