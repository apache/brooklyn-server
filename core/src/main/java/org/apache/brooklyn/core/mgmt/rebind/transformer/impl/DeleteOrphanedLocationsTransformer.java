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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathConstants;

import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMemento;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.mgmt.rebind.mementos.EntityMemento;
import org.apache.brooklyn.api.mgmt.rebind.mementos.LocationMemento;
import org.apache.brooklyn.core.mgmt.rebind.transformer.BrooklynMementoTransformer;
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.w3c.dom.Node;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Beta
public class DeleteOrphanedLocationsTransformer extends CompoundTransformer implements BrooklynMementoTransformer {

    protected DeleteOrphanedLocationsTransformer(DeleteOrphanedLocationsTransformer.Builder builder) {
        super(builder);
    }

    public static Builder builder() {
        return new DeleteOrphanedLocationsTransformer.Builder();
    }

    public static class Builder extends CompoundTransformer.Builder {

        @Override
        public DeleteOrphanedLocationsTransformer build() {
            return new DeleteOrphanedLocationsTransformer(this);
        }
    }

    @Override
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

    @Override
    public BrooklynMemento transform(BrooklynMemento input) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected Set<String> findReferencedLocationIds(BrooklynMemento input) {
        Set<String> result = Sets.newLinkedHashSet();

        for (EntityMemento entity : input.getEntityMementos().values()) {
            result.addAll(entity.getLocations());
        }
        return result;
    }

    @VisibleForTesting
    public Map<String, String> findLocationsToKeep(BrooklynMementoRawData input) {
        Map<String, String> locationsToKeep = MutableMap.of();
        Set<String> allReferencedLocations = findAllReferencedLocations(input);

        for (Map.Entry<String, String> location: input.getLocations().entrySet()) {
            if (allReferencedLocations.contains(location.getKey())) {
                locationsToKeep.put(location.getKey(), location.getValue());
            }
        }
        return locationsToKeep;
    }

    @VisibleForTesting
    public Set<String> findAllReferencedLocations(BrooklynMementoRawData input) {
        Set<String> allReferencedLocations = MutableSet.of();

        allReferencedLocations.addAll(searchLocationsToKeep(input.getEntities(), "/entity"));
        allReferencedLocations.addAll(searchLocationsToKeep(input.getPolicies(), "/policy"));
        allReferencedLocations.addAll(searchLocationsToKeep(input.getEnrichers(), "/enricher"));
        allReferencedLocations.addAll(searchLocationsToKeep(input.getFeeds(), "/feed"));
        allReferencedLocations.addAll(searchLocationsToKeepInLocations(input.getLocations(), allReferencedLocations));

        return allReferencedLocations;
    }

    protected Set<String> searchLocationsToKeep(Map<String, String> entities, String searchInTypePrefix) {
        Set<String> result = MutableSet.of();

        for(Map.Entry<String, String> entity: entities.entrySet()) {

            String location = "/locations/string";
            String locationProxy = "//locationProxy";

            result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(entity.getValue(), searchInTypePrefix+location, XPathConstants.NODESET)));
            result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(entity.getValue(), searchInTypePrefix+locationProxy, XPathConstants.NODESET)));
        }

        return result;
    }

    protected Set<String> searchLocationsToKeepInLocations(Map<String, String> locations, Set<String> alreadyReferencedLocations) {
        Set<String> result = MutableSet.of();

        String prefix = "/location";
        String locationId = "/id";
        String locationChildren = "/children/string";
        String locationParentDirectTag = "/parent";
        String locationProxy = "//locationProxy";

        for (Map.Entry<String, String> location: locations.entrySet()) {
            if (alreadyReferencedLocations.contains(location.getKey())) {
                result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(location.getValue(), prefix+locationId, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(location.getValue(), prefix+locationChildren, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(location.getValue(), prefix+locationParentDirectTag, XPathConstants.NODESET)));
                result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(location.getValue(), prefix+locationProxy, XPathConstants.NODESET)));
            }
        }

        return result;
    }

    protected Set<String> getAllNodesFromXpath(org.w3c.dom.NodeList nodeList) {
        Set<String> result = MutableSet.of();

        int i = 0;
        Node nextNode = nodeList.item(i);
        while (nextNode != null) {
            result.add(nextNode.getTextContent());
            nextNode = nodeList.item(++i);
        }

        return result;
    }

    protected Set<String> findLocationAncestors(BrooklynMemento input, String locationId) {
        Set<String> result = Sets.newLinkedHashSet();

        String parentId = null;
        do {
            LocationMemento memento = input.getLocationMemento(locationId);
            parentId = memento.getParent();
            if (parentId != null) result.add(parentId);
        } while (parentId != null);

        return result;
    }

    protected Set<String> findLocationDescendents(BrooklynMemento input, String locationId) {
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
