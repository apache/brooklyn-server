/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.brooklyn.test.osgi.entities;

import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.util.core.config.ConfigBag;

public class SimpleEffectorInitializer extends AddEffector {

    public SimpleEffectorInitializer() {
        super(newEffectorBuilder().build());
    }

    public static Effectors.EffectorBuilder<Void> newEffectorBuilder() {
        ConfigBag bag = ConfigBag.newInstance();
        bag.put(EFFECTOR_NAME, SimpleEffectorInitializer.class.getSimpleName());

        return AddEffector.newEffectorBuilder(Void.class, bag)
                .description("A bare-bones effector")
                .impl(new Body());
    }


    public static class Body extends EffectorBody<Void> {

        @Override
        public Void call(ConfigBag parameters) {
            return null;
        }

    }

}
