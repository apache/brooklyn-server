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
package org.apache.brooklyn.core.effector.util;

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.apache.brooklyn.core.effector.util.ChildrenBatchEffectorTest.DoNothingEffector.MUST_FAIL_AT;
import static org.apache.brooklyn.test.Asserts.*;

public class ChildrenBatchEffectorTest extends BrooklynAppUnitTestSupport {

    public final static Effector<String> EFFECTOR_PARENT = Effectors.effector(String.class, "childrenBatchEffector").buildAbstract();
    public final static Effector<String> EFFECTOR_CHILDREN = Effectors.effector(String.class, "children-effector").impl(new DoNothingEffector()).build();
    ChildrenBatchEffector cut;

    @BeforeMethod
    public void init() throws IOException {
        cut = null;
        DoNothingEffector.executionAttempts = 0;
        DoNothingEffector.correctExecutions = 0;
    }

    @Test
    public void testCorrectExecution() {

        cut = new ChildrenBatchEffector(ConfigBag.newInstance()
                .configure(ChildrenBatchEffector.BATCH_SIZE, 1)
                .configure(ChildrenBatchEffector.EFFECTOR_TO_INVOKE, EFFECTOR_CHILDREN.getName())
        );
        assertNotNull(cut);

        TestEntity testEntity = app.createAndManageChild(buildEntitySpec(cut));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class));

        assertEquals(testEntity.getChildren().size(), 3);

        Object output = testEntity.invoke(EFFECTOR_PARENT, ImmutableMap.of()).getUnchecked(Duration.minutes(1));
        assertTrue(output.toString().startsWith("Invoked 2"));
        assertEquals(DoNothingEffector.executionAttempts, 2);
        assertEquals(DoNothingEffector.correctExecutions, 2);
    }

    @Test
    public void testContinueInvocationsWhenOneEffectorFail() {

        cut = new ChildrenBatchEffector(ConfigBag.newInstance()
                .configure(ChildrenBatchEffector.BATCH_SIZE, 1)
                .configure(ChildrenBatchEffector.EFFECTOR_TO_INVOKE, EFFECTOR_CHILDREN.getName())
                .configure(ChildrenBatchEffector.FAIL_ON_EFFECTOR_FAILURE, false) // default value too
                .configure(ChildrenBatchEffector.EFFECTOR_ARGS, ImmutableMap.of(MUST_FAIL_AT.getName(), 2))
        );
        assertNotNull(cut);
        TestEntity testEntity = app.createAndManageChild(buildEntitySpec(cut));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class));
        assertEquals(testEntity.getChildren().size(), 4);

        Object output = testEntity.invoke(EFFECTOR_PARENT, ImmutableMap.of()).getUnchecked(Duration.minutes(1));

        assertTrue(output.toString().startsWith("Invoked 3"));
        assertEquals(DoNothingEffector.executionAttempts, 3);
        assertEquals(DoNothingEffector.correctExecutions, 2);
    }


    @Test(expectedExceptions = PropagatedRuntimeException.class)
    public void testExceptionWhenEffectorFailIfRequired() {

        cut = new ChildrenBatchEffector(ConfigBag.newInstance()
                .configure(ChildrenBatchEffector.BATCH_SIZE, 1)
                .configure(ChildrenBatchEffector.EFFECTOR_TO_INVOKE, EFFECTOR_CHILDREN.getName())
                .configure(ChildrenBatchEffector.FAIL_ON_EFFECTOR_FAILURE, true)
                .configure(ChildrenBatchEffector.EFFECTOR_ARGS, ImmutableMap.of(MUST_FAIL_AT.getName(), 2))
        );
        assertNotNull(cut);
        TestEntity testEntity = app.createAndManageChild(buildEntitySpec(cut));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class));
        assertEquals(testEntity.getChildren().size(), 3);

        Object output = testEntity.invoke(EFFECTOR_PARENT, ImmutableMap.of()).getUnchecked(Duration.minutes(1));
    }

    @Test
    public void testContinuesChildEffectorIsMissing() {

        cut = new ChildrenBatchEffector(ConfigBag.newInstance()
                .configure(ChildrenBatchEffector.BATCH_SIZE, 1)
                .configure(ChildrenBatchEffector.FAIL_ON_MISSING_EFFECTOR_TO_INVOKE, false) // default value
                .configure(ChildrenBatchEffector.EFFECTOR_TO_INVOKE, "unexisting")
        );
        assertNotNull(cut);
        TestEntity testEntity = app.createAndManageChild(buildEntitySpec(cut));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class));
        assertEquals(testEntity.getChildren().size(), 3);

        Object output = testEntity.invoke(EFFECTOR_PARENT, ImmutableMap.of()).getUnchecked(Duration.minutes(1));

        assertEquals(DoNothingEffector.executionAttempts, 0);
        assertEquals(DoNothingEffector.correctExecutions, 0);
    }

    @Test(expectedExceptions = PropagatedRuntimeException.class)
    public void testExceptionChildEffectorIsMissing() {

        cut = new ChildrenBatchEffector(ConfigBag.newInstance()
                .configure(ChildrenBatchEffector.BATCH_SIZE, 1)
                .configure(ChildrenBatchEffector.FAIL_ON_MISSING_EFFECTOR_TO_INVOKE, true)
                .configure(ChildrenBatchEffector.EFFECTOR_TO_INVOKE, "unexisting")
        );
        assertNotNull(cut);
        TestEntity testEntity = app.createAndManageChild(buildEntitySpec(cut));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(new AddEffector(EFFECTOR_CHILDREN)));
        testEntity.createAndManageChild(EntitySpec.create(TestEntity.class));
        assertEquals(testEntity.getChildren().size(), 3);

        Object output = testEntity.invoke(EFFECTOR_PARENT, ImmutableMap.of()).getUnchecked(Duration.minutes(1));
    }


    private EntitySpec<TestEntity> buildEntitySpec(EntityInitializer childrenBatchEffector) {
        return EntitySpec.create(TestEntity.class).addInitializer(childrenBatchEffector);
    }

    static class DoNothingEffector extends EffectorBody<String> {

        public static final ConfigKey<Integer> MUST_FAIL_AT = ConfigKeys.builder(Integer.class)
                .name("mustFailAt")
                .defaultValue(-1)
                .build();
        static int executionAttempts = 0;
        static int correctExecutions = 0;

        @Override
        public String call(ConfigBag config) {
            executionAttempts++;
            if (executionAttempts == config.get(MUST_FAIL_AT)) {
                throw new IllegalStateException();
            }
            correctExecutions++;
            return "OK";
        }
    }
}