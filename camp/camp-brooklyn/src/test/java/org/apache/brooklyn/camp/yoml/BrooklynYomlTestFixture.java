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
package org.apache.brooklyn.camp.yoml;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.annotations.YomlAnnotations;
import org.apache.brooklyn.util.yoml.tests.YomlTestFixture;

public class BrooklynYomlTestFixture extends YomlTestFixture {

    public static YomlTestFixture newInstance() { return new BrooklynYomlTestFixture(); }
    public static YomlTestFixture newInstance(YomlConfig config) { return new BrooklynYomlTestFixture(config); }
    public static YomlTestFixture newInstance(ManagementContext mgmt) {
        return newInstance(YomlTypePlanTransformer.newYomlConfig(mgmt, null).build());
    }

    public BrooklynYomlTestFixture() {}
    public BrooklynYomlTestFixture(YomlConfig config) {
        super(config);
    }
    
    @Override
    protected YomlAnnotations annotationsProvider() {
        return new BrooklynYomlAnnotations();
    }
    
}
