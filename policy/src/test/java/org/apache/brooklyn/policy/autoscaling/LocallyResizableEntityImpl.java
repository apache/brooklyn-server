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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.test.entity.TestCluster;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class LocallyResizableEntityImpl extends AbstractEntity implements LocallyResizableEntity {
    List<Integer> sizes = Lists.newArrayList();
    TestCluster cluster;
    long resizeSleepTime = 0;

    @Override
    public void init() {
        super.init();
        this.cluster = checkNotNull(config().get(TEST_CLUSTER), "testCluster");
        sensors().set(Startable.SERVICE_UP, true);
    }
    
    @Override
    public void setResizeSleepTime(long val) {
        resizeSleepTime = val;
    }
    
    @Override
    public List<Integer> getSizes() {
        return ImmutableList.copyOf(sizes);
    }
    
    @Override
    public Integer resize(Integer newSize) {
        try {
            Thread.sleep(resizeSleepTime);
            sizes.add(newSize); 
            return cluster.resize(newSize);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
    
    @Override
    public Integer getCurrentSize() {
        return cluster.getCurrentSize();
    }
    
    @Override
    public String toString() {
        return getDisplayName();
    }
}
