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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrooklynTagsRebindTest extends AbstractYamlRebindTest {
    @Test
    public void testRebindTags() throws Exception {
        final Entity entity = createAndStartApplication("services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.tags:",
                "  - 1",
                "  - hi world",
                "  - $brooklyn:object:",
                "      type: " + TagObject.class.getName());
        Assert.assertTrue(entity.tags().getTags().contains(1));
        Assert.assertTrue(entity.tags().getTags().contains("hi world"));
        Assert.assertTrue(Iterables.any(entity.tags().getTags(), new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return input instanceof TagObject;
            }
        }));
        rebind();
        final Entity newEntity = mgmt().getEntityManager().getEntity(entity.getId());
        Assert.assertTrue(newEntity.tags().getTags().contains(1));
        Assert.assertTrue(newEntity.tags().getTags().contains("hi world"));
        Assert.assertTrue(Iterables.any(newEntity.tags().getTags(), new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return input instanceof TagObject;
            }
        }));
    }

    public static class TagObject {}
}
