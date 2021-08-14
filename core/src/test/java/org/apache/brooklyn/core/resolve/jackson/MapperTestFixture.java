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
package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.yaml.Yamls;

public interface MapperTestFixture {

    ObjectMapper mapper();

    default String ser(Object v) {
        return ser(v, Object.class);
    }

    default <T> String ser(T v, Class<T> type) {
        try {
            return mapper().writerFor(type).writeValueAsString(v);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    default <T> String ser(T v, TypeToken<T> type) {
        try {
            return mapper().writerFor(BrooklynJacksonType.asTypeReference(type)).writeValueAsString(v);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    default <T> T deser(String v, Class<T> type) {
        try {
            return mapper().readValue(v, type);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }


    default <T> T deser(String v, TypeToken<T> type) {
        try {
            return mapper().readValue(v, BrooklynJacksonType.asTypeReference(type));
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    default <T> T deser(String v, RegisteredType type) {
        try {
            return mapper().readValue(v, BrooklynJacksonType.of(type));
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    default <T> T deser(String v) {
        return (T) deser(v, Object.class);
    }

    default String json(String yaml) {
        try {
            return BeanWithTypeUtils.newSimpleMapper().writeValueAsString( Iterables.getOnlyElement(Yamls.parseAll(yaml)) );
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    default <T> Maybe<T> resolve(Object o, Class<T> type) {
        BasicExecutionManager execManager = new BasicExecutionManager("test-context-"+ JavaClassNames.niceClassAndMethod());
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);

        return Tasks.resolving(o).as(type).context(execContext).deep().getMaybe();
    }

}
