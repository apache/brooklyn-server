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
package org.apache.brooklyn.core.test.entity;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.entity.group.DynamicClusterImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestSizeRecordingClusterImpl extends DynamicClusterImpl implements TestSizeRecordingCluster {

    public TestSizeRecordingClusterImpl () {
    }

    private final List<Integer> sizeHistory = Collections.synchronizedList(new ArrayList<Integer>());

    @Override
    public Integer resize(Integer desiredSize) {
        int existingSize = getCurrentSize();
        int newSize = super.resize(desiredSize);
        if (newSize != existingSize) {
            addSizeHistory(newSize);
        }
        return newSize;
    }

    @Override
    public List<Integer> getSizeHistory() {
        synchronized (sizeHistory) {
            return ImmutableList.copyOf(sizeHistory);
        }
    }

    private void addSizeHistory(int newSize) {
        synchronized (sizeHistory) {
            sizeHistory.add(newSize);
            sensors().set(SIZE_HISTORY_RECORD_COUNT, sizeHistory.size());
        }
    }

}
