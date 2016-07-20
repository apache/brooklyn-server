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

import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathConstants;

import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

@Beta
public class DeleteOrphanedStateTransformer extends CompoundTransformer {

    // TODO this isn't really a "CompoundTransformer" - it doesn't register any transformers 
    // in its builder, but instead overrides {@link #transform(BrooklynMementoRawData)}.
    // That is a convenient, concise way to get the behaviour we want. But it is a code smell!
    // We should probably either extract some common super-type, or another interface like
    // BrooklynMementoRawDataTransformer (but then what would call the latter? Would it be
    // a CompoundTransformer?!).
    
    private static final Logger LOG = LoggerFactory.getLogger(DeleteOrphanedStateTransformer.class);

    public static Builder builder() {
        return new DeleteOrphanedStateTransformer.Builder();
    }

    public static class Builder extends CompoundTransformer.Builder {
        @Override
        public DeleteOrphanedStateTransformer build() {
            return new DeleteOrphanedStateTransformer(this);
        }
    }

    protected DeleteOrphanedStateTransformer(DeleteOrphanedStateTransformer.Builder builder) {
        super(builder);
    }

    @Override
    public BrooklynMementoRawData transform(BrooklynMementoRawData input) {
        Map<String, String> locationsToKeep = findLocationsToKeep(input);
        Map<String, String> enrichersToKeep = copyRetainingKeys(input.getEnrichers(), findAllReferencedEnrichers(input));
        Map<String, String> policiesToKeep = copyRetainingKeys(input.getPolicies(), findAllReferencedPolicies(input));
        Map<String, String> feedsToKeep = copyRetainingKeys(input.getFeeds(), findAllReferencedFeeds(input));

        Set<String> locsToDelete = Sets.difference(input.getLocations().keySet(), locationsToKeep.keySet());
        Set<String> enrichersToDelete = Sets.difference(input.getEnrichers().keySet(), enrichersToKeep.keySet());
        Set<String> policiesToDelete = Sets.difference(input.getPolicies().keySet(), policiesToKeep.keySet());
        Set<String> feedsToDelete = Sets.difference(input.getFeeds().keySet(), feedsToKeep.keySet());
        LOG.info("Deleting {} orphaned location{}: {}", new Object[] {locsToDelete.size(), Strings.s(locsToDelete.size()), locsToDelete});
        LOG.info("Deleting {} orphaned enricher{}: {}", new Object[] {enrichersToDelete.size(), Strings.s(enrichersToDelete.size()), enrichersToDelete});
        LOG.info("Deleting {} orphaned polic{}: {}", new Object[] {policiesToDelete.size(), (policiesToDelete.size() == 1 ? "y" : "ies"), policiesToDelete});
        LOG.info("Deleting {} orphaned feed{}: {}", new Object[] {feedsToDelete.size(), Strings.s(feedsToDelete.size()), feedsToDelete});
        
        return BrooklynMementoRawData.builder()
                .brooklynVersion(input.getBrooklynVersion())
                .catalogItems(input.getCatalogItems())
                .entities(input.getEntities())
                .locations(locationsToKeep)
                .enrichers(enrichersToKeep)
                .policies(policiesToKeep)
                .feeds(feedsToKeep)
                .build();
    }

    protected Set<String> findAllReferencedEnrichers(BrooklynMementoRawData input) {
        return findAllReferencedAdjuncts(input.getEntities(), "entity" + "/enrichers/string");
    }

    protected Set<String> findAllReferencedPolicies(BrooklynMementoRawData input) {
        return findAllReferencedAdjuncts(input.getEntities(), "entity" + "/policies/string");
    }

    protected Set<String> findAllReferencedFeeds(BrooklynMementoRawData input) {
        return findAllReferencedAdjuncts(input.getEntities(), "entity" + "/feeds/string");
    }

    protected Set<String> findAllReferencedAdjuncts(Map<String, String> items, String xpath) {
        Set<String> result = Sets.newLinkedHashSet();

        for(Map.Entry<String, String> entry : items.entrySet()) {
            result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(entry.getValue(), xpath, XPathConstants.NODESET)));
        }

        return result;
    }

    @VisibleForTesting
    public Map<String, String> findLocationsToKeep(BrooklynMementoRawData input) {
        Set<String> allReferencedLocations = findAllReferencedLocations(input);
        return copyRetainingKeys(input.getLocations(), allReferencedLocations);
    }

    @VisibleForTesting
    public Set<String> findAllReferencedLocations(BrooklynMementoRawData input) {
        Set<String> result = Sets.newLinkedHashSet();

        result.addAll(searchLocationsToKeep(input.getEntities(), "/entity"));
        result.addAll(searchLocationsToKeep(input.getPolicies(), "/policy"));
        result.addAll(searchLocationsToKeep(input.getEnrichers(), "/enricher"));
        result.addAll(searchLocationsToKeep(input.getFeeds(), "/feed"));
        result.addAll(searchLocationsToKeepInLocations(input.getLocations(), result));

        return result;
    }

    protected Set<String> searchLocationsToKeep(Map<String, String> items, String searchInTypePrefix) {
        String locationsXpath = searchInTypePrefix+"/locations/string";
        String locationProxyXpath = searchInTypePrefix+"//locationProxy";

        Set<String> result = Sets.newLinkedHashSet();

        for(Map.Entry<String, String> entry : items.entrySet()) {
            result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(entry.getValue(), locationsXpath, XPathConstants.NODESET)));
            result.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(entry.getValue(), locationProxyXpath, XPathConstants.NODESET)));
        }

        return result;
    }

    protected Set<String> searchLocationsToKeepInLocations(Map<String, String> locations, Set<String> alreadyReferencedLocations) {
        Set<String> result = Sets.newLinkedHashSet();

        String prefix = "/location";
        String locationChildrenXpath = prefix+"/children/string";
        String locationParentDirectTagXpath = prefix+"/parent";
        String locationProxyXpath = prefix+"//locationProxy";

        Set<String> locsToInspect = alreadyReferencedLocations;
        
        while (locsToInspect.size() > 0) {
            Set<String> referencedLocs = Sets.newLinkedHashSet();
            for (String id : locsToInspect) {
                String xmlData = locations.get(id);
                if (xmlData != null) {
                    referencedLocs.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(xmlData, locationChildrenXpath, XPathConstants.NODESET)));
                    referencedLocs.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(xmlData, locationParentDirectTagXpath, XPathConstants.NODESET)));
                    referencedLocs.addAll(getAllNodesFromXpath((org.w3c.dom.NodeList) XmlUtil.xpath(xmlData, locationProxyXpath, XPathConstants.NODESET)));
                }
            }
            Set<String> newlyDiscoveredLocs = MutableSet.<String>builder()
                    .addAll(referencedLocs)
                    .removeAll(alreadyReferencedLocations)
                    .removeAll(result)
                    .build();
            result.addAll(newlyDiscoveredLocs);
            locsToInspect = newlyDiscoveredLocs;
        }

        return result;
    }

    protected <K, V> Map<K, V> copyRetainingKeys(Map<K, V> orig, Set<? extends K> keysToKeep) {
        Map<K, V> result = MutableMap.of();
        for (Map.Entry<K, V> entry : orig.entrySet()) {
            if (keysToKeep.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    protected Set<String> getAllNodesFromXpath(org.w3c.dom.NodeList nodeList) {
        Set<String> result = Sets.newLinkedHashSet();

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node nextNode = nodeList.item(i);
            if (nextNode != null) {
                result.add(nextNode.getTextContent());
            }
        }
        
        return result;
    }
}
