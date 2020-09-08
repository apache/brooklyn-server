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
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.util.core.config.ConfigBag;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

/** 
 * Entity initializer which adds an effector to an entity.
 * <p>
 * This instance provides a {@link #newEffectorBuilder(Class, ConfigBag)} 
 * which returns an abstract (body-less) effector defining:
 * <li> the name from {@link #EFFECTOR_NAME};
 * <li> the description from {@link #EFFECTOR_DESCRIPTION}
 * <li> the parameters from {@link #EFFECTOR_PARAMETER_DEFS}
 * <p>
 * Extenders may pass the effector to instantiate into the constructor.
 * Often subclasses will supply a constructor which takes a ConfigBag of parameters,
 * and a custom {@link #newEffectorBuilder(Class, ConfigBag)} which adds the body
 * before passing to this class.
 * <p>
 * Alternatively (recommended) extenders may override {@link #effector()} to recompute the effector each time,
 * and provide two constructors, a no-arg, and a 1-arg {@link ConfigBag} calling the super {@link ConfigBag} constructor here.
 * As per {@link org.apache.brooklyn.core.entity.EntityInitializers.InitializerPatternWithConfigBag} their code
 * may refer to {@link #initParam(ConfigKey)} to access parameters.
 * <p>
 * Note that the parameters passed to the call method in the body of the effector implementation
 * are only those supplied by a user at runtime; in order to merge with default
 * values, use {@link #getMergedParams(Effector, ConfigBag)}.
 *  
 * @since 0.7.0
 * @deprecated since 1.1 use {@link AddEffectorInitializerAbstract} because the static {@link Effector} constructor
 * does not lend itself to bean-with-type creation */
@Beta
@Deprecated
public class AddEffector extends AddEffectorInitializerAbstractProto {

    protected Effector<?> effector;

    public AddEffector(Effector<?> effector) {
        super();
        initEffector(Preconditions.checkNotNull(effector, "effector"));
    }

    protected AddEffector(ConfigBag params) {
        super(params);
    }

    // JSON deserialization constructor
    protected AddEffector() {}

    protected void init(ConfigBag params) {
        throw new IllegalStateException("Not supported as bean-with type; subclasses should overwrite this method");
    }

    protected void initEffector(Effector<?> effector) {
        this.effector = effector;
    }
    protected Effector<?> effector() {
        return effector;
    }

    public static <T> EffectorBuilder<T> newEffectorBuilder(Class<T> type, ConfigBag params) {
        return AddEffectorInitializerAbstractProto.newEffectorBuilder(type, params);
    }

}
