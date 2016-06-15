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
package org.apache.brooklyn.core.mgmt.rebind.transformer.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.org.apache.xml.internal.dtm.ref.DTMNodeList;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMemento;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.mgmt.rebind.mementos.EntityMemento;
import org.apache.brooklyn.api.mgmt.rebind.mementos.LocationMemento;
import org.apache.brooklyn.core.mgmt.rebind.dto.BrooklynMementoImpl;
import org.apache.brooklyn.core.mgmt.rebind.transformer.BrooklynMementoTransformer;
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathConstants;

@Beta
public class DeleteOrphanedLocationsTransformer extends CompoundTransformer implements BrooklynMementoTransformer {

    protected DeleteOrphanedLocationsTransformer(DeleteOrphanedLocationsTransformer.Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new DeleteOrphanedLocationsTransformer.Builder();
    }

    public static class Builder extends CompoundTransformer.Builder {

        public DeleteOrphanedLocationsTransformer build() {
            return new DeleteOrphanedLocationsTransformer(this);
        }
    }

    public BrooklynMementoRawData transform(BrooklynMementoRawData input) {
        Map<String, String> locationsToKeep = findLocationsToKeep(input);

        return BrooklynMementoRawData.builder()
                .catalogItems(input.getCatalogItems())
                .enrichers(input.getEnrichers())
                .entities(input.getEntities())
                .feeds(input.getFeeds())
                .locations(locationsToKeep)
                .policies(input.getPolicies())
                .build();
    }

    // TODO Work in progress; untested code!

    public BrooklynMemento transform(BrooklynMemento input) throws Exception {
        Set<String> referencedLocationIds = findReferencedLocationIds(input);
        Set<String> unreferencedLocationIds = Sets.newLinkedHashSet();
        List<String> toCheck = Lists.newLinkedList(input.getLocationIds());

        while (!toCheck.isEmpty()) {
            String locationId = toCheck.remove(0);
            List<String> locationsInHierarchy = MutableList.<String>builder()
                    .add(locationId)
                    .addAll(findLocationAncestors(input, locationId))
                    .addAll(findLocationDescendents(input, locationId))
                    .build();

            if (containsAny(referencedLocationIds, locationsInHierarchy)) {
                // keep them all
            } else {
                unreferencedLocationIds.addAll(locationsInHierarchy);
            }
            toCheck.removeAll(locationsInHierarchy);
        }

        // TODO What about brooklyn version?
        return BrooklynMementoImpl.builder()
                .applicationIds(input.getApplicationIds())
                .topLevelLocationIds(MutableSet.<String>builder()
                        .addAll(input.getTopLevelLocationIds())
                        .removeAll(unreferencedLocationIds)
                        .build())
                .entities(input.getEntityMementos())
                .locations(MutableMap.<String, LocationMemento>builder()
                        .putAll(input.getLocationMementos())
                        .removeAll(unreferencedLocationIds)
                        .build())
                .policies(input.getPolicyMementos())
                .enrichers(input.getEnricherMementos())
                .catalogItems(input.getCatalogItemMementos())
                .build();
    }

    public boolean containsAny(Collection<?> container, Iterable<?> contenders) {
        for (Object contender : contenders) {
            if (container.contains(contender)) return true;
        }
        return false;
    }

    public Set<String> findReferencedLocationIds(BrooklynMemento input) {
        Set<String> result = Sets.newLinkedHashSet();

        for (EntityMemento entity : input.getEntityMementos().values()) {
            result.addAll(entity.getLocations());
        }
        return result;
    }

    public Map<String, String> findLocationsToKeep(BrooklynMementoRawData input) {
        Map<String, String> locationsToKeep = MutableMap.of();
        Set<String> allReferencedLocations = findAllReferencedLocations(input);

        for (Map.Entry location: input.getLocations().entrySet()) {
            if (allReferencedLocations.contains(location.getKey())) {
                locationsToKeep.put((String) location.getKey(), (String) location.getValue());
            }
        }
        return locationsToKeep;
    }

    public Set<String> findAllReferencedLocations(BrooklynMementoRawData input) {
        Set<String> allReferencedLocations = MutableSet.of();

        allReferencedLocations.addAll(searchLocationsToKeepInEntities(input.getEntities()));
        allReferencedLocations.addAll(searchLocationsToKeepInPolicies(input.getPolicies()));
        allReferencedLocations.addAll(searchLocationsToKeepInEnrichers(input.getEnrichers()));
        allReferencedLocations.addAll(searchLocationsToKeepInLocations(input.getLocations(), allReferencedLocations));

        return allReferencedLocations;
    }

    private Set<String> searchLocationsToKeepInEntities(Map<String, String> entities) {
        Set<String> result = MutableSet.of();

        for(Map.Entry entity: entities.entrySet()) {

            String prefix = "/entity";
            String location = "/locations/string";
            String locationProxy = "/attributes/softwareservice.provisioningLocation/locationProxy";

            result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) entity.getValue(), prefix+location, XPathConstants.NODESET)));
            result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) entity.getValue(), prefix+locationProxy, XPathConstants.NODESET)));
        }

        return result;
    }

    private Set<String> searchLocationsToKeepInLocations(Map<String, String> locations, Set<String> alreadyReferencedLocations) {
        Set<String> result = MutableSet.of();

        String prefix = "/location";
        String locationId = "/id";
        String locationChildren = "/children/string";
        String locationParentDirectTag = "/parent";
        String locationParent = "/locationConfig/jcloudsParent/locationProxy";
        String locationProxy = "/locationConfig/vmInstanceIds/map/entry/locationProxy";

        for (Map.Entry location: locations.entrySet()) {
            if (alreadyReferencedLocations.contains(location)) {
                result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) location.getValue(), prefix+locationId, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) location.getValue(), prefix+locationChildren, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) location.getValue(), prefix+locationParent, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) location.getValue(), prefix+locationParentDirectTag, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((DTMNodeList) XmlUtil.xpath((String) location.getValue(), prefix+locationProxy, XPathConstants.NODESET)));
            }
        }

        return result;
    }

    private Set<String> searchLocationsToKeepInPolicies(Map<String, String> policies) {
        Set<String> result = MutableSet.of();

        return result;
    }

    private Set<String> searchLocationsToKeepInEnrichers(Map<String, String> enrichers) {
        Set<String> result = MutableSet.of();

        return result;
    }

    private Set<String> getAllNodesFromXpath(DTMNodeList nodeList) {
        Set<String> result = MutableSet.of();

        int i = 0;
        Node nextNode = nodeList.item(i);
        while (nextNode != null) {
            result.add(nextNode.getTextContent());
            nextNode = nodeList.item(++i);
        }

        return result;
    }

    public Set<String> findLocationAncestors(BrooklynMemento input, String locationId) {
        Set<String> result = Sets.newLinkedHashSet();

        String parentId = null;
        do {
            LocationMemento memento = input.getLocationMemento(locationId);
            parentId = memento.getParent();
            if (parentId != null) result.add(parentId);
        } while (parentId != null);

        return result;
    }

    public Set<String> findLocationDescendents(BrooklynMemento input, String locationId) {
        Set<String> result = Sets.newLinkedHashSet();
        List<String> tovisit = Lists.newLinkedList();

        tovisit.add(locationId);
        while (!tovisit.isEmpty()) {
            LocationMemento memento = input.getLocationMemento(tovisit.remove(0));
            List<String> children = memento.getChildren();
            result.addAll(children);
            tovisit.addAll(children);
        };

        return result;
    }
}
