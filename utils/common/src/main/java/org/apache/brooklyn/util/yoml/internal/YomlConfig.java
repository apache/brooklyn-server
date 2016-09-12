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
package org.apache.brooklyn.util.yoml.internal;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;

import com.google.common.collect.ImmutableList;

public interface YomlConfig {

    public YomlTypeRegistry getTypeRegistry();
    public TypeCoercer getCoercer();
    public List<YomlSerializer> getSerializersPost();
    public ConstructionInstruction getConstructionInstruction();

    public static class BasicYomlConfig implements YomlConfig {
        private BasicYomlConfig() {}
        private BasicYomlConfig(YomlConfig original) {
            this.typeRegistry = original.getTypeRegistry();
            this.coercer = original.getCoercer();
            this.serializersPost = original.getSerializersPost();
            this.constructionInstruction = original.getConstructionInstruction();
        }

        private YomlTypeRegistry typeRegistry;
        private TypeCoercer coercer = TypeCoercerExtensible.newDefault();
        private List<YomlSerializer> serializersPost = MutableList.of();
        private ConstructionInstruction constructionInstruction;

        public YomlTypeRegistry getTypeRegistry() {
            return typeRegistry;
        }

        public TypeCoercer getCoercer() {
            return coercer;
        }

        public List<YomlSerializer> getSerializersPost() {
            return ImmutableList.copyOf(serializersPost);
        }

        public ConstructionInstruction getConstructionInstruction() {
            return constructionInstruction;
        }
    }


    public static class Builder {
        public static Builder builder() { return new Builder(); }
        public static Builder builder(YomlConfig source) { return new Builder(source); }
        
        final BasicYomlConfig result;
        protected Builder() { result = new BasicYomlConfig(); }
        protected Builder(YomlConfig source) { result = new BasicYomlConfig(source); }
        public Builder typeRegistry(YomlTypeRegistry tr) { result.typeRegistry = tr; return this; }
        public Builder coercer(TypeCoercer x) { result.coercer = x; return this; }
        public Builder serializersPost(List<YomlSerializer> x) { result.serializersPost = x; return this; }
        public Builder constructionInstruction(ConstructionInstruction x) { result.constructionInstruction = x; return this; }
        
        public YomlConfig build() { return new BasicYomlConfig(result); }
    }
}
