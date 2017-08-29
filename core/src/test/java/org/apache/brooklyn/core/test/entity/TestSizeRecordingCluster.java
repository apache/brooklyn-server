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

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.group.DynamicCluster;

import java.util.List;

/**
 * Similar to {@link TestCluster}, however the intercepts are simply used to record the resize, and not
 * mock them, instead delegating to {@link DynamicCluster}
 */
@ImplementedBy(TestSizeRecordingClusterImpl.class)
public interface TestSizeRecordingCluster extends DynamicCluster {

    AttributeSensor<Integer> SIZE_HISTORY_RECORD_COUNT = Sensors.newIntegerSensor("size.history.count", "Number of entries in the size history record");

    List<Integer> getSizeHistory();

}
