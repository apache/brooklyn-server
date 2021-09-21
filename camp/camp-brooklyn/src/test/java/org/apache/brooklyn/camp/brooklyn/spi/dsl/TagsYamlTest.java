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

import org.apache.brooklyn.entity.stock.BasicEntity;
import static org.testng.Assert.assertTrue;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class TagsYamlTest extends AbstractYamlTest {
    @Test
    public void testBrooklynCampSingleTag() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "    - hi");
        assertTrue(app.tags().getTags().contains("hi"));
    }

    @Test
    public void testBrooklynCampMultipleTags() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - tag1",
                "  - \"2\"",
                "  - \"- 3\"");
        assertTrue(app.tags().getTags().contains("tag1"));
        assertTrue(app.tags().getTags().contains("2"));
        assertTrue(app.tags().getTags().contains("- 3"));
    }

    @Test
    public void testBrooklynCampTagsFailNonList() throws Exception {
        try {
            final Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicApplication.class.getName(),
                    "  brooklyn.tags:",
                    "    tag1: true",
                    "    tag2: 2");
            Asserts.shouldHaveFailedPreviously("Should throw IllegalArgumentException exception; instead got: "+app);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "brooklyn.tags must be a list");
            Asserts.assertStringContainsAtLeastOne(Exceptions.getFirstInteresting(e).getMessage(),
                "brooklyn.tags must be a list, is: ");
        }
    }

    @Test
    public void testBrooklynCampKnowsIntegerTags() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - tag1",
                "  - 3");
        assertTrue(app.tags().getTags().contains(3));
        assertTrue(app.tags().getTags().contains("tag1"));
    }

    @Test
    public void testBrooklynCampObjectTags() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - tag1",
                "  - $brooklyn:object:",
                "      type: " + TagsTestObject.class.getName());
        assertTrue(Iterables.any(app.tags().getTags(), new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object input) {
                return input instanceof TagsTestObject;
            }
        }));
        assertTrue(app.tags().getTags().contains("tag1"));
    }

    @Test
    public void testBrooklynCampFailDslTags() throws Exception {
        try {
            final Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicApplication.class.getName(),
                    "  brooklyn.tags:",
                    "  - tag1",
                    "  - $brooklyn:object:",
                    "      type: "+TagsTestObject.class.getName(),
                    "      constructor.args:",
                    "      - $brooklyn:attributeWhenReady(\"host.name\")");
            Asserts.shouldHaveFailedPreviously("Should throw IllegalArgumentException exception; instead got "+app);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, 
                "brooklyn.tags should not contain DeferredSupplier",
                "A DeferredSupplier is made when using $brooklyn:attributeWhenReady");
            Asserts.assertStringContainsAtLeastOne(Exceptions.getFirstInteresting(e).getMessage(),
                "brooklyn.tags should not contain DeferredSupplier");
        }
    }

    @Test
    public void testTagWithDslValue() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - $brooklyn:formatString(\"myval\")");
        assertTrue(app.tags().getTags().contains("myval"));
    }

    @Test
    public void testBrooklynCampApplicationTag() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "tags:",
                "- oldStyle",
                "brooklyn.tags:",
                "- newStyle");
        assertTrue(app.tags().getTags().contains("oldStyle"));
        assertTrue(app.tags().getTags().contains("newStyle"));
    }

    @Test
    public void testBrooklynCampApplicationNewStyleOnlyTag() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "brooklyn.tags:",
                "- newStyle");
        assertTrue(app.tags().getTags().contains("newStyle"));
    }

    @Test
    public void testBrooklynCampApplicationOldStyleOnlyTag() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "tags:",
                "- oldStyle");
        assertTrue(app.tags().getTags().contains("oldStyle"));
    }

    public static class TagsTestObject {
        public TagsTestObject() {}
        public TagsTestObject(Object arg1) {}
    }
}
