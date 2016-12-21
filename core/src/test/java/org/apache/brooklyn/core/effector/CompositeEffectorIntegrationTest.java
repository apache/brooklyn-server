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
package org.apache.brooklyn.core.effector;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.effector.http.HttpCommandEffector;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class CompositeEffectorIntegrationTest {

    final static Effector<String> EFFECTOR_START = Effectors.effector(String.class, "start").buildAbstract();

    private TestApplication app;
    private EntityLocal entity;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class).location(TestApplication.LOCALHOST_MACHINE_SPEC));
        app.start(ImmutableList.<Location>of());
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());
    }

    @Test(groups="Integration")
    public void testCompositeEffector() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "eff1")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/users/apache")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET"))
                .apply(entity);
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "eff2")
                .configure(HttpCommandEffector.EFFECTOR_URI, "https://api.github.com/users/brooklyncentral")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET"))
                .apply(entity);
        new CompositeEffector(ConfigBag.newInstance()
                .configure(CompositeEffector.EFFECTOR_NAME, "start")
                .configure(CompositeEffector.EFFECTORS, ImmutableList.of("eff1", "eff2")))
                .apply(entity);

        String val = entity.invoke(EFFECTOR_START, MutableMap.<String,String>of()).get();
        // TODO
        System.out.println(val);
    }

}
