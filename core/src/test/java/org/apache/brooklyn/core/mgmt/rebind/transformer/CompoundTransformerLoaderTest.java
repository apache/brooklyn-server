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
package org.apache.brooklyn.core.mgmt.rebind.transformer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Collection;

import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.mgmt.rebind.transformer.impl.XsltTransformer;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class CompoundTransformerLoaderTest {

    @Test
    public void testLoadsTransformerFromYaml() throws Exception {
        String contents =
                "- renameType:\n"+
                "    old_val: myoldname\n"+
                "    new_val: mynewname\n"+
                "- renameClassTag:\n"+
                "    old_val: myoldname\n"+
                "    new_val: mynewname\n"+
                "- renameField:\n"+
                "    class_name: myclassname\n"+
                "    old_val: myoldname\n"+
                "    new_val: mynewname\n"+
                // low-level mechanism to change catalogItemId; used (and tested) by higher-level methods
                // which use symbolic_name:version notation to avoid the unpleasant need for yaml quotes
                "- catalogItemId:\n"+
                "    old_symbolic_name: myclassname\n"+
                "    new_symbolic_name: myclassname\n"+
                "    new_version: '2.0'\n"+
                "- xslt:\n"+
                "    url: classpath://brooklyn/entity/rebind/transformer/impl/renameType.xslt\n"+
                "    substitutions:\n"+
                "      old_val: myoldname\n"+
                "      new_val: mynewname\n"+
                "- rawDataTransformer:\n"+
                "    type: "+MyRawDataTransformer.class.getName()+"\n";
        
        CompoundTransformer transformer = CompoundTransformerLoader.load(contents);
        Collection<RawDataTransformer> rawDataTransformers = transformer.getRawDataTransformers().get(BrooklynObjectType.ENTITY);
        assertTrue(Iterables.get(rawDataTransformers, 0) instanceof XsltTransformer);
        assertTrue(Iterables.get(rawDataTransformers, 1) instanceof XsltTransformer);
        assertTrue(Iterables.get(rawDataTransformers, 2) instanceof XsltTransformer);
        assertTrue(Iterables.get(rawDataTransformers, 3) instanceof XsltTransformer);
        assertTrue(Iterables.get(rawDataTransformers, 4) instanceof XsltTransformer);
        assertTrue(Iterables.get(rawDataTransformers, 5) instanceof MyRawDataTransformer);
    }
    
    @Test
    public void testLoadsDeletionsFromYaml() throws Exception {
        String contents =
                "- deletions:\n"+
                "    catalog:\n"+
                "    - cat1\n"+
                "    - cat2\n"+
                "    entities:\n"+
                "    - ent1\n"+
                "    - ent2\n"+
                "    locations:\n"+
                "    - loc1\n"+
                "    - loc2\n"+
                "    enrichers:\n"+
                "    - enricher1\n"+
                "    - enricher2\n"+
                "    policies:\n"+
                "    - pol1\n"+
                "    - pol2\n"+
                "    feeds:\n"+
                "    - feed1\n"+
                "    - feed2\n";
        
        CompoundTransformer transformer = CompoundTransformerLoader.load(contents);
        
        Multimap<BrooklynObjectType, String> deletions = transformer.getDeletions();
        HashMultimap<BrooklynObjectType, String> expected = HashMultimap.create();
        expected.putAll(BrooklynObjectType.CATALOG_ITEM, ImmutableSet.of("cat1", "cat2"));
        expected.putAll(BrooklynObjectType.ENTITY, ImmutableSet.of("ent1", "ent2"));
        expected.putAll(BrooklynObjectType.LOCATION, ImmutableSet.of("loc1", "loc2"));
        expected.putAll(BrooklynObjectType.POLICY, ImmutableSet.of("pol1", "pol2"));
        expected.putAll(BrooklynObjectType.ENRICHER, ImmutableSet.of("enricher1", "enricher2"));
        expected.putAll(BrooklynObjectType.FEED, ImmutableSet.of("feed1", "feed2"));
        assertEquals(deletions, expected);
    }
    
    @Test
    public void testLoadFailsIfInvalidDeletionTypeFromYaml() throws Exception {
        String contents =
                "- deletions:\n"+
                "    wrong:\n"+
                "    - cat1\n";
        
        try {
            CompoundTransformer transformer = CompoundTransformerLoader.load(contents);
            Asserts.shouldHaveFailedPreviously("transformer="+transformer);
        } catch (IllegalStateException e) {
            Asserts.expectedFailureContains(e, "Unsupported transform");
        }
    }
    
    public static class MyRawDataTransformer implements RawDataTransformer {
        @Override
        public String transform(String input) throws Exception {
            return input; // no-op
        }
    }
}
