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
package org.apache.brooklyn.core.mgmt.rebind;

import java.io.File;
import java.io.FileReader;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

public class RebindConfigInheritanceTest extends RebindTestFixtureWithApp {

    private static final Logger log = LoggerFactory.getLogger(RebindConfigInheritanceTest.class);

    ConfigKey<String> key1 = ConfigKeys.builder(String.class, "key1").runtimeInheritance(BasicConfigInheritance.NEVER_INHERITED).build();
    String origMemento, newMemento;
    Application rebindedApp;
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }
        
    protected void doReadConfigInheritance(String label, String entityId) throws Exception {
        String mementoFilename = "config-inheritance-"+label+"-"+entityId;
        origMemento = Streams.readFullyString(getClass().getResourceAsStream(mementoFilename));
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(origMemento.getBytes(), persistedEntityFile);
        
        // we'll make assertions on what we've loaded
        rebind();
        rebindedApp = (Application) newManagementContext.lookup(entityId);
        
        // we'll also make assertions on the contents written out
        RebindTestUtils.waitForPersisted(mgmt());
        newMemento = Joiner.on("\n").join(Files.readLines(persistedEntityFile, Charsets.UTF_8));
    }
    
    /** also see {@link RebindWithDeserializingClassRenamesTest} */
    @Test
    public void testPreBasicConfigInheritance_2016_07() throws Exception {
        doReadConfigInheritance("prebasic-2016-07", "toruf2wxg4");
        
        ConfigKey<?> k = Iterables.getOnlyElement( rebindedApp.config().findKeysDeclared(ConfigPredicates.nameEqualTo("my.config.inheritanceMerged")) );
        
        Asserts.assertStringContains(origMemento, "<parentInheritance class=\"org.apache.brooklyn.config.ConfigInheritance$Merged\"/>");
        Asserts.assertStringDoesNotContain(origMemento, BasicConfigInheritance.DEEP_MERGE.getClass().getName());

        // should now convert it to BasicConfigInheritance.DEEP_MERGE
        Asserts.assertStringDoesNotContain(newMemento, "ConfigInheritance$Merged");
        Asserts.assertStringDoesNotContain(newMemento, "ConfigInheritance$Legacy$Merged");
        Asserts.assertStringContains(newMemento, BasicConfigInheritance.DEEP_MERGE.getClass().getName());
        
        ConfigInheritance inh = k.getInheritanceByContext(InheritanceContext.RUNTIME_MANAGEMENT);
        Assert.assertEquals(inh, BasicConfigInheritance.DEEP_MERGE);
    }
    
    @Test
    public void testBasicConfigInheritance_2016_10() throws Exception {
        doReadConfigInheritance("basic-2016-10", "wj5s8u9h73");

        checkNewAppNonInheritingKey1(rebindedApp);
        
        Asserts.assertStringContains(origMemento, "isReinherited");
        Asserts.assertStringDoesNotContain(origMemento, "NEVER_INHERITED");
        Asserts.assertStringDoesNotContain(origMemento, "NeverInherited");
        
        // should write it out as NeverInherited
        Asserts.assertStringDoesNotContain(newMemento, "isReinherited");
        Asserts.assertStringContains(newMemento, "NeverInherited");
    }

    @Test
    public void testReadConfigInheritance_2016_11() throws Exception {
        doReadConfigInheritance("basic-2016-11", "kmpez5fznt");
        checkNewAppNonInheritingKey1(rebindedApp);
        
        String origMementoTidied = origMemento.substring(origMemento.indexOf("<entity>"));
        origMementoTidied = Strings.replaceAllNonRegex(origMementoTidied, "VERSION", BrooklynVersion.get());
        Asserts.assertEquals(origMementoTidied, newMemento);
    }
    
    @Test
    public void testBasicConfigInheritanceProgrammatic() throws Exception {
        origApp.config().set(key1, "1");
        
        rebind();
        
        String entityFile = Streams.readFully(new FileReader(new File(mementoDir, "entities/"+origApp.getApplicationId())));
        log.info("persisted file with config inheritance programmatic:\n"+entityFile);
        
        checkNewAppNonInheritingKey1(newApp);
    }

    protected void checkNewAppNonInheritingKey1(Application app) {
        // check key
        EntityAsserts.assertConfigEquals(app, key1, "1");
        
        // check not inherited
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class));
        EntityAsserts.assertConfigEquals(entity, key1, null);
    }

}
