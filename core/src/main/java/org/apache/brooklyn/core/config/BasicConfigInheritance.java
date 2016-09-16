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
package org.apache.brooklyn.core.config;

import java.util.Iterator;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;

public class BasicConfigInheritance implements ConfigInheritance {

    private static final long serialVersionUID = -5916548049057961051L;

    // TODO javadoc
    public static BasicConfigInheritance NOT_REINHERITED = new BasicConfigInheritance(false,"overwrite",false);
    public static BasicConfigInheritance NEVER_INHERITED = new BasicConfigInheritance(false,"overwrite",true);
    public static BasicConfigInheritance OVERWRITE = new BasicConfigInheritance(true,"overwrite",false);
    public static BasicConfigInheritance DEEP_MERGE = new BasicConfigInheritance(true,"deep_merge",false);
    
//    reinheritable? true/false; if false, children/descendants/inheritors will never see it; default true
    protected final boolean isReinherited;
//    conflict-resolution-strategy? overwrite or deep_merge or null; default null meaning caller supplies
    protected final String conflictResolutionStrategy;
//    use-local-default-value? true/false; if true, overwrite above means "always ignore"; default false
    protected final boolean useLocalDefaultValue;

    protected BasicConfigInheritance(boolean isReinherited, String conflictResolutionStrategy, boolean useLocalDefaultValue) {
        super();
        this.isReinherited = isReinherited;
        this.conflictResolutionStrategy = conflictResolutionStrategy;
        this.useLocalDefaultValue = useLocalDefaultValue;
    }

    @SuppressWarnings("deprecation")
    @Override
    public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
        return null;
    }
    
    @Override
    public <T> ContainerAndValue<T> resolveInheriting(
            Iterator<ContainerAndKeyValue<T>> containerAndDataThroughAncestors,
            ConfigInheritanceContext context) {
//        XXX;
        return null;
    }

}


