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
package org.apache.brooklyn.core.workflow.steps.variables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonType;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;

import java.util.List;
import java.util.Set;

public class TransformJsonish extends WorkflowTransformDefault {

    boolean json;   // RW using json
    boolean yaml;   // RW using yaml
    boolean bash;   // output as bash (implies json and string, unless otherwise specified)
    boolean parse;  // if a string supplied, it will be parsed
    boolean string; // if object is not a string, it is converted to a string
    boolean encode; // object is stringified, whether a string or not

    public TransformJsonish(boolean json, boolean yaml, boolean bash) {
        this.json = json;
        this.yaml = yaml;
        this.bash = bash;
    }

    @Override
    protected void initCheckingDefinition() {
        Set<String> d = MutableSet.copyOf(definition);
        json |= d.remove("json");
        yaml |= d.remove("yaml");
        if (yaml && json) throw new IllegalArgumentException("Cannot specify both 'yaml' and 'json'");

        bash |= d.remove("bash");
        if (yaml && bash) throw new IllegalArgumentException("Cannot specify 'yaml' for 'bash' output; use JSON");

        parse |= d.remove("parse");
        if (parse && bash) throw new IllegalArgumentException("Cannot specify 'parse' with 'bash'");

        encode |= d.remove("encode");
        if (parse && encode) throw new IllegalArgumentException("Cannot specify both 'parse' and 'encode'");

        string |= d.remove("string");
        if (parse && string) throw new IllegalArgumentException("Cannot specify both 'parse' and 'string'");

        if (!d.isEmpty()) throw new IllegalArgumentException("Unsupported json/yaml arguments: " + d);
    }

    @Override
    public Object apply(Object v) {
        try {
            ObjectMapper mapper;
            if (yaml)
                mapper = BeanWithTypeUtils.newYamlMapper(context.getManagementContext(), true, null, true);
            else if (bash || json)
                mapper = BeanWithTypeUtils.newMapper(context.getManagementContext(), true, null, true);
            else
                throw new IllegalArgumentException("Expected json/yaml/bash");  // shouldn't come here

            /*
          * `json [string|parse|encode]`: indicates the input should be converted as needed using JSON;
            if `string` is specified, the value is returned if it is a string, or serialized as a JSON string if it is anything else;
            if `parse` is specified, the result of `string` is then parsed to yield maps/lists/strings/primitives (any string value must be valid json);
            if `encode` is specified, the value is serialized as a JSON string as per JS "stringify", including encoding string values wrapped in explicit `"`;
            if nothing is specified the behavior is as per `parse`, but any value which is already a map/list/primitive is passed-through unchanged

             */
            boolean stringSupplied = v instanceof String;
            boolean generated = false;

            if (v!=null && !encode && yaml && stringSupplied) {
                v = Yamls.lastDocumentFunction().apply((String) v);
            }

            boolean serializeNeeded =
                    // always serialize in encode mode
                    encode ? true
                    // serialize if not already a string for any of these modes
                    : parse || string || bash ? !stringSupplied
                    // or in default serialize if it isn't a string or a natural json object
                    : !Jsonya.isJsonPrimitiveDeep(v);
            if (serializeNeeded) {
                v = mapper.writeValueAsString(v);
                generated = true;
            }

            boolean readNeeded =
                    // never read in encode mode
                    encode ? false
                    // always read in parse more
                    : parse ? true
                    // if string or bash (not encode) is wanted, don't read (when not encoding)
                    : string || bash ? false
                    // otherwise (default) read if we generated it or if it was a string
                    : stringSupplied || generated;

            if (readNeeded) {
                // when parse specified explicitly, we need to read it in to an object holder
                v = mapper.readValue((String) v, BrooklynJacksonType.asTypeReference(
                        TypeToken.of(JsonPassThroughDeserializer.JsonObjectHolder.class))).value;
            }

            if (shouldTrim(v, generated)) v = ((String) v).trim();

            if (bash) {
                // currently always wraps for bash; could make it based on the output
                v = StringEscapes.BashStringEscapes.wrapBash("" + v);
            }

        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }

        return v;
    }

    private boolean shouldTrim(Object resultCoerced, boolean generated) {
        if (generated && resultCoerced instanceof String) {
            // automatically trim in some generated cases, to make output nicer to work with
            String rst = ((String) resultCoerced).trim();
            if (rst.endsWith("\"") || rst.endsWith("'")) return true;
            else if (yaml && (rst.endsWith("+") || rst.endsWith("|")))
                return false;  //yaml trailing whitespace signifier
            else return !Strings.isMultiLine(rst);     //lastly trim if something was generated and is multiline
        }
        return false;
    }
}
