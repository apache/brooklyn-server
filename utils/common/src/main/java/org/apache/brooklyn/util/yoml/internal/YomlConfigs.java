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

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.serializers.FieldsInMapUnderFields;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeEnum;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistry;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeList;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeMap;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypePrimitive;

import com.google.common.collect.ImmutableList;

public class YomlConfigs {

    private static class BasicYomlConfig implements YomlConfig {
        private BasicYomlConfig() {}
        private BasicYomlConfig(YomlConfig original) {
            if (original!=null) {
                this.typeRegistry = original.getTypeRegistry();
                this.coercer = original.getCoercer();
                this.serializersPost = original.getSerializersPost();
                this.constructionInstruction = original.getConstructionInstruction();
            }
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

    public static class Builder<T extends Builder<?>> {
        
        final BasicYomlConfig result;
        protected Builder() { result = new BasicYomlConfig(); }
        protected Builder(YomlConfig source) { result = new BasicYomlConfig(source); }
        
        @SuppressWarnings("unchecked")
        T thiz = (T) this;
        public T typeRegistry(YomlTypeRegistry tr) { result.typeRegistry = tr; return thiz; }
        public T coercer(TypeCoercer x) { result.coercer = x; return thiz; }
        public T serializersPostReplace(List<YomlSerializer> x) { result.serializersPost = x; return thiz; }
        public T serializersPostAdd(Collection<YomlSerializer> x) { result.serializersPost.addAll(x); return thiz; }
        public T serializersPostAddDefaults() { return serializersPostAdd(getDefaultSerializers()); }
        public T constructionInstruction(ConstructionInstruction x) { result.constructionInstruction = x; return thiz; }
        
        public YomlConfig build() { return new BasicYomlConfig(result); }
        
        public static List<YomlSerializer> getDefaultSerializers() {
            return MutableList.<YomlSerializer>of(
                new FieldsInMapUnderFields(),
                new InstantiateTypePrimitive(),
                new InstantiateTypeEnum(),
                new InstantiateTypeList(),
                new InstantiateTypeMap(),
                new InstantiateTypeFromRegistry() );
        }
    }
}
