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

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.util.core.config.ConfigBag;

/**
 * Entity initializer which adds an effector to an entity.
 * <p>
 * This parent instance provides parameter management and a {@link #newAbstractEffectorBuilder(Class)}
 * which returns an abstract (body-less) effector defining:
 * <li> the name from {@link #EFFECTOR_NAME};
 * <li> the description from {@link #EFFECTOR_DESCRIPTION}
 * <li> the parameters from {@link #EFFECTOR_PARAMETER_DEFS}
 * <p>
 * The main thing needed to extend this is to implement {@link #newEffectorBuilder()}, calling {@link #newAbstractEffectorBuilder(Class)} if desired,
 * and supplying their effector body.
 * <p>
 * Extenders should supply a no-arg constructor (can be private) for use during deserializing YAML,
 * and a {@link ConfigBag} constructor calling to super to allow programmatic creation.
 * <p>
 * Extenders may provide additional {@link org.apache.brooklyn.config.ConfigKey} statics and use them like normal.
 * <p>
 * Note that the parameters passed to the call method in the body of the effector implementation
 * are only those supplied by a user at runtime; in order to merge with parameters supplied at effector definition time,
 * use {@link #getMergedParams(Effector, ConfigBag)}.
 * <p>
 * Also note that although fields can be used deserialization from YAML to registered types is not always fully supported.
 * If strongly typed deserialization is desired and {@link org.apache.brooklyn.util.core.flags.TypeCoercions} not sufficient,
 * routines in {@link org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils} can be applied.
 *  
 * @since 0.7.0 */
@Beta
public abstract class AddEffectorInitializerAbstract extends AddEffectorInitializerAbstractProto {

    protected AddEffectorInitializerAbstract() {}
    protected AddEffectorInitializerAbstract(ConfigBag params) {
        super(params);
    }

    protected Effector<?> effector() {
        return newEffectorBuilder().build();
    }

    protected abstract <T> EffectorBuilder<T> newEffectorBuilder();

    // TODO support TypeToken here ... and anyone who looks this up dynamically should use BrooklynTypeNameResolver
    protected <T> EffectorBuilder<T> newAbstractEffectorBuilder(Class<T> type) {
        return AddEffectorInitializerAbstractProto.newEffectorBuilder(type, initParams());
    }

}
