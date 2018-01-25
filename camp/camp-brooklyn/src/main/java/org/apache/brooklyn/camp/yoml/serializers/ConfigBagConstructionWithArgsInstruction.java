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
package org.apache.brooklyn.camp.yoml.serializers;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions.WrappingConstructionInstruction;

import com.google.common.collect.Iterables;

/** Like {@link ConfigKeyMapConstructionWithArgsInstruction} but passing a {@link ConfigBag} as the single argument
 * to the constructor. */
public class ConfigBagConstructionWithArgsInstruction extends ConfigKeyMapConstructionWithArgsInstruction {

    public ConfigBagConstructionWithArgsInstruction(Class<?> type, Map<String, Object> values,
            WrappingConstructionInstruction optionalOuter, Map<String, ConfigKey<?>> keysByAlias) {
        super(type, values, optionalOuter, keysByAlias);
    }
    
    @Override
    protected List<?> combineArguments(List<ConstructionInstruction> constructorsSoFarOutermostFirst) {
        return MutableList.of(
            ConfigBag.newInstance(
                (Map<?,?>)Iterables.getOnlyElement( super.combineArguments(constructorsSoFarOutermostFirst) )));
    }
    
}
