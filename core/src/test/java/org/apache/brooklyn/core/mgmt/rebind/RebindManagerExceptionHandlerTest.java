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

import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.ConverterLookup;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.core.ClassLoaderReference;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationNoEnrichersImpl;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.Tasks.ForTestingAndLegacyCompatibilityOnly.LegacyDeepResolutionMode;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.stream.Collectors;

public class RebindManagerExceptionHandlerTest extends RebindTestFixtureWithApp {

    @XStreamConverter(BadConverter.class)
    static class NotDeserializable {
    }
    public static class BadConverter implements Converter {
        public BadConverter(ConverterLookup lookup, ClassLoaderReference loader) {
        }

        @Override
        public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        }

        @Override
        public Object unmarshal(HierarchicalStreamReader hierarchicalStreamReader, UnmarshallingContext unmarshallingContext) {
            throw new IllegalStateException("Deliberate failure");
        }

        @Override
        public boolean canConvert(Class type) {
            return NotDeserializable.class.equals(type);
        }
    }

    @Override
    protected TestApplication createApp() {
        TestApplication app = super.createApp();
        app.config().set(TestEntity.CONF_OBJECT, new NotDeserializable());
        return app;
    }

    @Test
    public void testRebindFailFailure() throws Throwable {
        try {
            RebindOptions rebindOptions = RebindOptions.create().defaultExceptionHandler();
            rebind(rebindOptions);
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "Deliberate failure");
        }
    }

    @Test
    public void testRebindFailContinue() throws Throwable {
        TestApplication app2 = super.createApp();
        Asserts.assertEquals(mgmt().getEntityManager().getEntities().stream().map(Entity::getId).collect(Collectors.toSet()), MutableSet.of(app().getId(), app2.getId()));

        RebindOptions rebindOptions = RebindOptions.create()
                .defaultExceptionHandler()
                .additionalProperties(ImmutableMap.of(
                        // prior to 2022-12 we tested ADD_CONFIG handler, but now that never throws (we don't coerce on rebind)
                        RebindManagerImpl.REBIND_FAILURE_MODE.getName(), "continue"));
        TestApplication rebindedApp = rebind(rebindOptions);

        // app1 not rebinded due to failure

        Asserts.assertEquals(mgmt().getEntityManager().getEntities().stream().map(Entity::getId).collect(Collectors.toSet()), MutableSet.of(app2.getId()));
        Asserts.assertEquals(rebindedApp.getId(), app2.getId());
    }

}
