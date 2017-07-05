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
package org.apache.brooklyn.policy.autoscaling;

import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Resizable;
import org.apache.brooklyn.core.test.entity.TestCluster;

/**
 * Test class for providing a Resizable LocallyManagedEntity for policy testing
 * It is hooked up to a TestCluster that can be used to make assertions against
 */
@ImplementedBy(LocallyResizableEntityImpl.class)
public interface LocallyResizableEntity extends Entity, Resizable {
    ConfigKey<TestCluster> TEST_CLUSTER = ConfigKeys.newConfigKey(
            TestCluster.class,
            "testCluster");
    
    void setResizeSleepTime(long val);
    
    List<Integer> getSizes();
}
