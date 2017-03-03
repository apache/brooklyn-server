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
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.xstream.XmlUtil;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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

    /**
     * We inspect the state in two different ways to find the entities that will be deleted:
     * belt-and-braces! Only if the state is not referenced by either of those two approaches
     * will we delete it.
     */
    @Override
    public BrooklynMementoRawData transform(BrooklynMementoRawData input) {
        ReferencedState stateReferencedFromXpath = new ReachabilityXpathInspector().inspect(input);
        ReferencedState stateToKeepFromGrep = new ReachabilityGrepInspector().inspect(input);
        ReferencedState stateToKeepFromXpath = stateReferencedFromXpath.filterForExtant(input);
        ReferencedState.warnOfDifferences(stateToKeepFromXpath, stateToKeepFromGrep);

        ReferencedState stateToKeep = ReferencedState.union(stateToKeepFromXpath, stateToKeepFromGrep);
        
        Map<String, String> locationsToKeep = copyRetainingKeys(input.getLocations(), stateToKeep.locations);
        Map<String, String> enrichersToKeep = copyRetainingKeys(input.getEnrichers(), stateToKeep.enrichers);
        Map<String, String> policiesToKeep = copyRetainingKeys(input.getPolicies(), stateToKeep.policies);
        Map<String, String> feedsToKeep = copyRetainingKeys(input.getFeeds(), stateToKeep.feeds);

        Set<String> locsToDelete = Sets.difference(input.getLocations().keySet(), locationsToKeep.keySet());
        Set<String> enrichersToDelete = Sets.difference(input.getEnrichers().keySet(), enrichersToKeep.keySet());
        Set<String> policiesToDelete = Sets.difference(input.getPolicies().keySet(), policiesToKeep.keySet());
        Set<String> feedsToDelete = Sets.difference(input.getFeeds().keySet(), feedsToKeep.keySet());
        LOG.info("Deleting {} orphaned location{} (of {}): {}", new Object[] {locsToDelete.size(), Strings.s(locsToDelete.size()), input.getLocations().size(), locsToDelete});
        LOG.info("Deleting {} orphaned enricher{} (of {}): {}", new Object[] {enrichersToDelete.size(), Strings.s(enrichersToDelete.size()), input.getEnrichers().size(), enrichersToDelete});
        LOG.info("Deleting {} orphaned polic{} (of {}): {}", new Object[] {policiesToDelete.size(), (policiesToDelete.size() == 1 ? "y" : "ies"), input.getPolicies().size(), policiesToDelete});
        LOG.info("Deleting {} orphaned feed{} (of {}): {}", new Object[] {feedsToDelete.size(), Strings.s(feedsToDelete.size()), input.getFeeds().size(), feedsToDelete});
        
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

    /**
     * Searches the state, based on using xpath to find references, starting at the roots of
     * reachability. 
     * 
     * For locations, we find all locations referenced by entities, enrichers, policies or feeds 
     * (looking at the xpath for the locations or location refs in the persisted state). We then 
     * search through those locations for other locations reachable by them, and so on.
     * 
     * We also keep any location that is of type {@link PortForwardManager}.
     */
    protected static class ReachabilityXpathInspector {
        public ReferencedState inspect(BrooklynMementoRawData input) {
            return new ReferencedState()
                    .locations(findAllReferencedLocations(input))
                    .enrichers(findAllReferencedEnrichers(input))
                    .policies(findAllReferencedPolicies(input))
                    .feeds(findAllReferencedFeeds(input));
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
    
        protected Set<String> findAllReferencedLocations(BrooklynMementoRawData input) {
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
            String locationTypeXpath = prefix+"/type";
            String locationChildrenXpath = prefix+"/children/string";
            String locationParentDirectTagXpath = prefix+"/parent";
            String locationProxyXpath = prefix+"//locationProxy";
    
            Set<String> locsToInspect = MutableSet.copyOf(alreadyReferencedLocations);
    
            // Keep org.apache.brooklyn.core.location.access.PortForwardManager, even if not referenced.
            // It is found and loaded by PortForwardManagerLocationResolver.
            for (Map.Entry<String, String> entry : locations.entrySet()) {
                String locId = entry.getKey();
                if (!alreadyReferencedLocations.contains(locId)) {
                    String locType = XmlUtil.xpath(entry.getValue(), locationTypeXpath);
                    if (locType != null && locType.contains("PortForwardManager")) {
                        result.add(locId);
                        locsToInspect.add(locId);
                    }
                }
            }
    
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
    
    /**
     * Searches the state, based on state.contains(id). We don't care about the structure or where
     * in the state the id was mentioned.
     * 
     * The rules of reachability (in terms of the roots used etc) are the same as for 
     * {@link ReachabilityXpathInspector}.
     */
    protected static class ReachabilityGrepInspector {
        protected ReferencedState inspect(BrooklynMementoRawData input) {
            Set<String> locations = Sets.newLinkedHashSet();
            Set<String> feeds = Sets.newLinkedHashSet();
            Set<String> enrichers = Sets.newLinkedHashSet();
            Set<String> policies = Sets.newLinkedHashSet();

            for (String id : input.getEnrichers().keySet()) {
                if (isMentionedBy(id, BrooklynObjectType.ENTITY, input)) {
                    enrichers.add(id);
                }
            }
            for (String id : input.getPolicies().keySet()) {
                if (isMentionedBy(id, BrooklynObjectType.ENTITY, input)) {
                    policies.add(id);
                }
            }
            for (String id : input.getFeeds().keySet()) {
                if (isMentionedBy(id, BrooklynObjectType.ENTITY, input)) {
                    feeds.add(id);
                }
            }
            
            // Initial pass of locations (from the roots of reachability)
            for (Map.Entry<String, String> entry : input.getLocations().entrySet()) {
                String id = entry.getKey();
                String locationState = entry.getValue();
                if (locationState.contains("PortForwardManager")) {
                    locations.add(id);
                    continue;
                }
                for (BrooklynObjectType type : ImmutableList.of(BrooklynObjectType.ENTITY, BrooklynObjectType.ENRICHER, BrooklynObjectType.POLICY, BrooklynObjectType.FEED, BrooklynObjectType.CATALOG_ITEM)) {
                    if (isMentionedBy(id, type, input)) {
                        locations.add(id);
                        break;
                    }
                }
            }
    
            // Find the transitive reachabily locations from those we have already reached.
            Set<String> locsToInspect = MutableSet.copyOf(locations);
            
            while (locsToInspect.size() > 0) {
                Map<String, String> locStateToInspect = MutableMap.copyOf(input.getLocations());
                locStateToInspect.keySet().retainAll(locsToInspect);
 
                Set<String> unreachedLocs = Sets.difference(input.getLocations().keySet(), locations);
                Set<String> newlyDiscoveredLocs = Sets.newLinkedHashSet();

                for (String id : unreachedLocs) {
                    if (isMentionedBy(id, locStateToInspect.values())) {
                        newlyDiscoveredLocs.add(id);
                    }
                }
                locations.addAll(newlyDiscoveredLocs);
                locsToInspect = newlyDiscoveredLocs;
            }
    
            return new ReferencedState()
                    .locations(locations)
                    .enrichers(enrichers)
                    .policies(policies)
                    .feeds(feeds);
        }
        
        protected boolean isMentionedBy(String id, BrooklynObjectType type, BrooklynMementoRawData input) {
            return isMentionedBy(id, input.getObjectsOfType(type).values());
        }
        
        protected boolean isMentionedBy(String id, Iterable<String> states) {
            for (String state : states) {
                if (state.contains(id)) {
                    return true;
                }
            }
            return false;
        }
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

    protected static class ReferencedState {
        public static ReferencedState union(ReferencedState s1, ReferencedState s2) {
            ReferencedState result = new ReferencedState();
            result.locations(ImmutableSet.copyOf(Iterables.concat(s1.locations, s2.locations)));
            result.enrichers(ImmutableSet.copyOf(Iterables.concat(s1.enrichers, s2.enrichers)));
            result.policies(ImmutableSet.copyOf(Iterables.concat(s1.policies, s2.policies)));
            result.feeds(ImmutableSet.copyOf(Iterables.concat(s1.feeds, s2.feeds)));
            return result;
        }
        
        public static void warnOfDifferences(ReferencedState s1, ReferencedState s2) {
            Set<String> locDiffs = Sets.symmetricDifference(s1.locations, s2.locations);
            Set<String> enricherDiffs = Sets.symmetricDifference(s1.enrichers, s2.enrichers);
            Set<String> policyDiffs = Sets.symmetricDifference(s1.policies, s2.policies);
            Set<String> feedDiffs = Sets.symmetricDifference(s1.feeds, s2.feeds);
            
            if (locDiffs.size() > 0) {
                LOG.warn("Deletion of orphan state found unusually referenced locations (keeping): " + locDiffs);
            }
            if (enricherDiffs.size() > 0) {
                LOG.warn("Deletion of orphan state found unusually referenced enrichers (keeping): " + enricherDiffs);
            }
            if (policyDiffs.size() > 0) {
                LOG.warn("Deletion of orphan state found unusually referenced policies (keeping): " + policyDiffs);
            }
            if (feedDiffs.size() > 0) {
                LOG.warn("Deletion of orphan state found unusually referenced feeds (keeping): " + feedDiffs);
            }
        }

        protected Set<String> locations = ImmutableSet.of();
        protected Set<String> feeds = ImmutableSet.of();
        protected Set<String> enrichers = ImmutableSet.of();
        protected Set<String> policies = ImmutableSet.of();
        
        public ReferencedState filterForExtant(BrooklynMementoRawData input) {
            ReferencedState result = new ReferencedState();
            result.locations(Sets.intersection(locations, input.getLocations().keySet()));
            result.enrichers(Sets.intersection(enrichers, input.getEnrichers().keySet()));
            result.policies(Sets.intersection(policies, input.getPolicies().keySet()));
            result.feeds(Sets.intersection(feeds, input.getFeeds().keySet()));
            return result;
        }

        protected ReferencedState locations(Set<String> vals) {
            locations = vals;
            return this;
        }
        protected ReferencedState feeds(Set<String> vals) {
            feeds = vals;
            return this;
        }
        protected ReferencedState enrichers(Set<String> vals) {
            enrichers = vals;
            return this;
        }
        protected ReferencedState policies(Set<String> vals) {
            policies = vals;
            return this;
        }
    }
}
