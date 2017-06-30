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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Time;
import org.osgi.framework.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

/** Tests ability to rebind to previous version serialization state, for various classes; 
 * and utils to create such state (edit the config key value then run main method). 
 */
public class MiscClassesRebindTest extends RebindTestFixtureWithApp {

    private static final ConfigKey<Object> TEST_KEY = ConfigKeys.builder(Object.class).name("testKey").build();
    private static final Logger log = LoggerFactory.getLogger(MiscClassesRebindTest.class);

    /** Method to facilitate creation of memento files */
    private void createMemento() throws Exception {
        setUp();
        origApp = super.createApp();
        
        // edit this, run this class's main method, then use the log output for your test case
        origApp.config().set(TEST_KEY,  new VersionedName("foo", Version.parseVersion("1.0.0.foo")));

        
        RebindTestUtils.stopPersistence(origApp);
        String fn = mementoDir + File.separator + "entities" + File.separator + origApp.getApplicationId();
        log.info("Persisted to "+fn);
        String yyyyMM = Time.makeDateString(new Date(), "yyyy-MM");
        log.info("Set up your tests by copying from the persistence dir "+mementoDir+"\n\n"+
            "cp "+fn+" "+
            "src/test/resources/"+getClass().getPackage().getName().replaceAll("\\.", "/")+"/"+
            JavaClassNames.cleanSimpleClassName(this)+"-"+yyyyMM+"-entity-"+origApp.getApplicationId()+".memento\n");
        String content = Streams.readFullyString(new FileInputStream(new File(fn)));
        log.info("Or paste the following contents there:\n"+content);
        log.info("Then add the apache comment header there, and write your test doing  loadEntityMemento(\""+yyyyMM+"\", \""+origApp.getApplicationId()+"\")");
    }
    
    public static void main(String[] args) throws Exception {
        new MiscClassesRebindTest().createMemento();
        // halt to keep memento dir
        Runtime.getRuntime().halt(0);
    }
    
    
    public TestApplication createApp() { /* no-op here for most tests */ return null; }
    
    protected Entity loadEntityMemento(String label, String entityId) throws Exception {
        String mementoResourceName = JavaClassNames.cleanSimpleClassName(this) + "-" + label + "-entity-" + entityId+".memento";
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoResourceName));
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(memento.getBytes(), persistedEntityFile);
        
        return newApp = rebind();
    }

    protected String getEntityMementoContent() throws InterruptedException, TimeoutException, FileNotFoundException {
        RebindTestUtils.stopPersistence(newApp);
        String fn = mementoDir + File.separator + "entities" + File.separator + newApp.getApplicationId();
        return Streams.readFullyString(new FileInputStream(new File(fn)));
    }

    @SuppressWarnings("unchecked")
    protected <T> T getTestKeyFromEntityMemento(String label, String entityId, Class<T> type) throws Exception {
        Entity e = loadEntityMemento(label, entityId);
        return (T) e.getConfig(TEST_KEY);
    }

    
    @Test
    public void testVersionedName() throws Exception {
        VersionedName vn = getTestKeyFromEntityMemento("2017-06-versionedname", "n7p20t5h4o", VersionedName.class);
        Assert.assertEquals(vn, new VersionedName("foo", "1.0.0-foo"));
        Assert.assertNotEquals(vn, new VersionedName("foo", "1.0.0.foo"));
        Assert.assertTrue(vn.equalsOsgi(new VersionedName("foo", Version.parseVersion("1.0.0.foo"))));
        
        String newEntityContent = getEntityMementoContent();
        // log.info("New VN persistence is\n"+newEntityContent);
        Asserts.assertStringContains(newEntityContent, "1.0.0-foo");
        // phrases from original persisted state are changed
        Asserts.assertStringDoesNotContain(newEntityContent, "<symbolicName>foo");
        Asserts.assertStringDoesNotContain(newEntityContent, "<version>1.0.0");
        Asserts.assertStringDoesNotContain(newEntityContent, "1.0.0.foo");
        // they should now be this
        Asserts.assertStringContains(newEntityContent, "<name>foo");
        Asserts.assertStringContains(newEntityContent, "<v>1.0.0-foo");
    }

}

