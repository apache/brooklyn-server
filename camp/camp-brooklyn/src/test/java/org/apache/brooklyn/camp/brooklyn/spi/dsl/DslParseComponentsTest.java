/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.BasicCampPlatform;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class DslParseComponentsTest extends AbstractYamlTest {
    
    private static final ConfigKey<Object> DEST = ConfigKeys.newConfigKey(Object.class, "dest");
    private static final ConfigKey<Object> DEST2 = ConfigKeys.newConfigKey(Object.class, "dest2");

    private static final Logger log = LoggerFactory.getLogger(DslParseComponentsTest.class);
    
    Entity app = null;
    
    protected Entity app() throws Exception {
        if (app==null) {
            app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicApplication.class.getName(),
                    "  id: one",
                    "  brooklyn.config:",
                    "    dest: 1",
                    "    dest2: 1",
                    "  brooklyn.children:",
                    "  - type: "+BasicEntity.class.getName(),
                    "    id: two",
                    "    brooklyn.config:",
                    "      dest2: 2"
                    );
        }
        return app;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        app = null;
        super.tearDown();
    }
    
    private Entity find(String desiredComponentId) {
        return Iterables.tryFind(mgmt().getEntityManager().getEntities(), EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, desiredComponentId)).orNull();
    }
    
    public Object parseDslExpression(String input) {
        String s1 = Yamls.getAs( Yamls.parseAll( Streams.reader(Streams.newInputStreamWithContents(input)) ), String.class );
        BasicCampPlatform p = new BasicCampPlatform();
        p.pdp().addInterpreter(new BrooklynDslInterpreter());
        Object out = p.pdp().applyInterpreters(MutableMap.of("key", s1)).get("key");
        log.debug("parsed "+input+" as "+out+" ("+(out==null ? "null" : out.getClass())+")");
        return out;
    }
    
    @Test
    public void testTestSetup() throws Exception {
        app();
        Assert.assertEquals(find("two").getConfig(DEST), 1);
        Assert.assertEquals(find("two").getConfig(DEST2), 2);
    }
    
    @Test
    public void testDslParsingAndFormatStringEvaluation() throws Exception {
        // format string evaluates immediately
        Assert.assertEquals(parseDslExpression("$brooklyn:formatString(\"hello %s\", \"world\")"), "hello world");
    }
    
    @Test
    public void testConfigParsingAndToString() throws Exception {
        String x1 = "$brooklyn:config(\"dest\")";
        Object y1 = parseDslExpression(x1);
        // however config is a deferred supplier, with a toString in canonical form
        Asserts.assertInstanceOf(y1, BrooklynDslDeferredSupplier.class);
        Assert.assertEquals(y1.toString(), x1);
    }

    @Test
    public void testConfigEvaluation() throws Exception {
        app();
        
        Object y1 = parseDslExpression("$brooklyn:config(\"dest\")");
        String y2 = Tasks.resolveValue(y1, String.class, ((EntityInternal) find("two")).getExecutionContext());
        Assert.assertEquals(y2.toString(), "1");
    }

    @Test
    public void testFormatStringWithConfig() throws Exception {
        app();
        
        Object y1 = parseDslExpression("$brooklyn:formatString(\"%s-%s\", config(\"dest\"), $brooklyn:config(\"dest2\"))");
        Assert.assertEquals(y1.toString(), "$brooklyn:formatString(\"%s-%s\", config(\"dest\"), config(\"dest2\"))");
        
        String y2 = Tasks.resolveValue(y1, String.class, ((EntityInternal) find("two")).getExecutionContext());
        Assert.assertEquals(y2.toString(), "1-2");
        
        String y3 = Tasks.resolveValue(y1, String.class, ((EntityInternal) find("one")).getExecutionContext());
        Assert.assertEquals(y3.toString(), "1-1");
    }

    @Test
    public void testEntityReferenceAndAttributeWhenReady() throws Exception {
        app();
        find("one").sensors().set(Attributes.ADDRESS, "1");
        find("two").sensors().set(Attributes.ADDRESS, "2");
        
        Object y1 = parseDslExpression("$brooklyn:formatString(\"%s-%s\", "
            + "parent().attributeWhenReady(\"host.address\"), "
            + "$brooklyn:attributeWhenReady(\"host.address\"))");
        Assert.assertEquals(y1.toString(), "$brooklyn:formatString(\"%s-%s\", "
            + "parent().attributeWhenReady(\"host.address\"), "
            + "attributeWhenReady(\"host.address\"))");
        
        String y2 = Tasks.resolveValue(y1, String.class, ((EntityInternal) find("two")).getExecutionContext());
        Assert.assertEquals(y2.toString(), "1-2");
        
        Object z1 = parseDslExpression("$brooklyn:formatString(\"%s-%s\", "
            + "entity(\"one\").descendant(\"two\").attributeWhenReady(\"host.address\"), "
            + "component(\"two\").entity(entityId()).attributeWhenReady(\"host.address\"))");
        Assert.assertEquals(z1.toString(), "$brooklyn:formatString(\"%s-%s\", "
            + "entity(\"one\").descendant(\"two\").attributeWhenReady(\"host.address\"), "
            + "entity(entityId()).attributeWhenReady(\"host.address\"))");
        
        String z2 = Tasks.resolveValue(z1, String.class, ((EntityInternal) find("one")).getExecutionContext());
        Assert.assertEquals(z2.toString(), "2-1");
    }

}
