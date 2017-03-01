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
package org.apache.brooklyn.camp.brooklyn.catalog;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;

/** simple effector which returns the contents of a file, used to test resolution of */
public class GetFileContentsEffector extends AddEffector {

    final static ConfigKey<String> FILENAME = ConfigKeys.newStringConfigKey("filename");
    final static Effector<String> GET_FILE_CONTENTS = Effectors.effector(String.class, "getFileContents").parameter(FILENAME).buildAbstract();
    
    public GetFileContentsEffector() {
        super(Effectors.effector(GET_FILE_CONTENTS).impl(new Body()).build());
    }

    static class Body extends EffectorBody<String> {
        @Override
        public String call(ConfigBag parameters) {
            return ResourceUtils.create(entity()).getResourceAsString( parameters.get(FILENAME) );
        }
    }
}
