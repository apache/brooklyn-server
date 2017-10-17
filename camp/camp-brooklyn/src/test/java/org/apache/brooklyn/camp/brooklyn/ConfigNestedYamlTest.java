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
package org.apache.brooklyn.camp.brooklyn;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class ConfigNestedYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ConfigNestedYamlTest.class);
    
    @Test
    public void testSimpleYamlBlueprint() throws Exception {
        doTestWithBlueprint( loadYaml("config-nested-test.yaml"), false );
    }

    @Test
    public void testCatalogNoParameter() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        doTestWithBlueprint( "services: [ { type: test-no-parameter } ]", false);
    }

    @Test
    public void testCatalogParameterFromSuperYamlType() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        Entity ent = doTestWithBlueprint( "services: [ { type: test-map-parameter } ]", false);
        Dumper.dumpInfo(ent);
    }

    @Test
    public void testCatalogParameterFromSuperYamlTypeAfterTemplateAccess() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        Entity ent = makeBlueprint( "services: [ { type: test-map-parameter } ]");
        
        checkEntity(ent, false);
    }

    protected void checkTemplates(EntityInternal ent) {
        Map<String, Object> substitutions = MutableMap.copyOf(ent.config().get(SoftwareProcess.TEMPLATE_SUBSTITUTIONS)).asUnmodifiable();
        String out0 = TemplateProcessor.processTemplateContents("hello "+"${field}", substitutions);
        Assert.assertEquals(out0, "hello val");
        
        String out1 = TemplateProcessor.processTemplateContents("hello "+"${config['template.substitutions']['field']}", ent, substitutions);
        Assert.assertEquals(out1, "hello val");
    }

    @Test
    public void testCatalogParameterFromSuperYamlTypeInCluster() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        Entity cluster = makeBlueprint("services: [ { type: test-cluster-with-map-parameter } ]");
        Dumper.dumpInfo(cluster.getApplication());
        Entity parentInCluster = Iterables.getOnlyElement( ((DynamicCluster)cluster).getMembers() );
        Entity target = Iterables.getOnlyElement(parentInCluster.getChildren());
        checkEntity( target, false );
    }
    

    @Test
    public void testCatalogParameterFromSuperYamlTypeAsSoftware() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        Entity ent = doTestWithBlueprint( "services: [ { type: test-map-parameter-software } ]", false);
        Dumper.dumpInfo(ent);
    }

    @Test
    public void testCatalogNoParameterAsSoftware() throws Exception {
        addCatalogItems( loadYaml("config-nested-test.bom") );
        doTestWithBlueprint( "services: [ { type: test-no-parameter-software } ]", true);
    }

    protected Entity doTestWithBlueprint(String bp, boolean expectDotSyntaxToWork) throws Exception {
        return checkEntity(makeBlueprint(bp), expectDotSyntaxToWork);
    }
    
    protected Entity makeBlueprint(String bp) throws Exception {
        Entity app = createAndStartApplication( bp );
        waitForApplicationTasks(app);
        return Iterables.getOnlyElement( app.getChildren() );
    }
    
    protected Entity checkEntity(Entity ent, boolean expectDotSyntaxToWork) throws Exception {
        String dotSyntax = ent.getConfig( ConfigKeys.newStringConfigKey("template.substitutions.field") );
        Assert.assertEquals(dotSyntax, expectDotSyntaxToWork ? "val" : null, "wrong value for dot syntax: "+dotSyntax);
        // observed in some situations we get the dot syntax instead!
        // Assert.assertEquals(dotSyntax, "val", "wrong value for dot syntax: "+dotSyntax);
        
        // might be okay if the above does happen (though I can't get it reproduced);
        // however the following must _always_ work
        
        // strongly typed
        Map<?,?> map1 = ent.getConfig(SoftwareProcess.TEMPLATE_SUBSTITUTIONS);
        Assert.assertNotNull(map1, "Nothing found for 'template.substitutions'");
        Assert.assertTrue(map1.containsKey("field"), "missing 'field' in "+map1);
        Assert.assertEquals(map1.get("field"), "val", "wrong value for 'field' in "+map1);
        
        // because SoftwareProcess declares TEMPLATE_SUBSTITUTIONS it rewrites where map entries are put
        // (to use dot syntax) so if the entity is a software process (ie expectDotSyntaxToWork)
        // neither "anonymous key" lookup nor template processing deep-map works,
        // unless we switch from the query key to own key when we do the key value lookup
        // (we should probably be doing that any way however!)
        
        // (those two tests deliberately fail in this commit, fixed in following commit)
        
        // anonymous key
        Map<?,?> map2 = ent.getConfig(ConfigKeys.newConfigKey(Map.class, "template.substitutions")); 
        Assert.assertNotNull(map2, "Nothing found for 'template.substitutions'");
        Assert.assertTrue(map2.containsKey("field"), "missing 'field' in "+map2);
        Assert.assertEquals(map2.get("field"), "val", "wrong value for 'field' in "+map2);
        
        checkTemplates((EntityInternal) ent);
        
        return ent;
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
