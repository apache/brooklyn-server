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
package org.apache.brooklyn.util.yorml.tests;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.apache.brooklyn.util.yorml.serializers.ExplicitField;
import org.apache.brooklyn.util.yorml.tests.YormlBasicTests.Shape;
import org.testng.annotations.Test;

/** Tests that explicit fields can be set at the outer level in yaml. */
public class ExplicitFieldTests {

    /*

serializers can come from:
     * default pre (any?)
     * declared type
     * calling context
     * expected type
     * default post - fields in fields and instantiate type


- id: java-shape-defaulting-square
  type: java:Shape
  serializers:
  - type: explicit-field
    field-name: name
    aliases: [ shape-name ]
    default: square   # alternative to above
  definition:  # body, yaml, item, content
    ...

- id: shape
  type: java:Shape
  serializers:
  - type: explicit-field
    field-name: name
    aliases: [ shape-name ]
  - type: explicit-field
    field-name: color
    aliases: [ shape-color ]
- id: yaml-shape-defaulting-square
  type: shape
  serializers:
  - type: explicit-field
    field-name: name
    default: square
    # ensure if shape-name set it overrides 'square'
- id: red-square
  type: square
  fields:
    shape-color: red
  # read red-square, but writes { type: shape, name: square, color: red }
  # except in context expecting shape it writes { name: square, color: red }
  #   and in context expecting square it writes { color: red }

 
# sub-type serializers go first, also before expected-type serializers
# but...
# 1) if we read something with shape-name do we do a setField("name", "square") ?
#    NO: defaults request RERUN_IF_CHANGED if there are fields to read present, only apply when no fields to read present
# 2) if java fields contains 'name: square' do we write it?
#    NO: defaults write to a defaults map if not present
#    and field writers don't write if a defaults map contains the default value
# so on write explicit-fields will
# * populate a key in the DEFAULTS map if not present
# and on init
# * keep a list of mangles/aliases
# and on read
# * look up all mangles/aliases once the type is known
# * error if there are multiple mangles/aliases with different values
# (inefficient for that to run multiple times but we'll live with that)
     */

    public static YormlSerializer explicitFieldSerializer(String yaml) {
        return (YormlSerializer) YormlTestFixture.newInstance().read("{ fields: "+yaml+" }", "java:"+ExplicitField.class.getName()).lastReadResult;
    }

    protected static YormlTestFixture simpleExplicitFieldFixture() {
        return YormlTestFixture.newInstance().
            addType("shape", Shape.class, MutableList.of(explicitFieldSerializer("{ fieldName: name }")));
    }
    
    static String SIMPLE_IN_WITHOUT_TYPE = "{ name: diamond, fields: { color: black } }";
    static Shape SIMPLE_OUT = new Shape().name("diamond").color("black");
    
    
    @Test
    public void testReadExplicitField() {
        simpleExplicitFieldFixture().
        read( SIMPLE_IN_WITHOUT_TYPE, "shape" ).
        assertResult( SIMPLE_OUT );
    }
    @Test
    public void testWriteExplicitField() {
        simpleExplicitFieldFixture().
        write( SIMPLE_OUT, "shape" ).
        assertResult( SIMPLE_IN_WITHOUT_TYPE );
    }

    static String SIMPLE_IN_WITH_TYPE = "{ type: shape, name: diamond, fields: { color: black } }";

    @Test
    public void testReadExplicitFieldNoExpectedType() {
        simpleExplicitFieldFixture().
        read( SIMPLE_IN_WITH_TYPE, null ).
        assertResult( SIMPLE_OUT);
    }
    @Test
    public void testWriteExplicitFieldNoExpectedType() {
        simpleExplicitFieldFixture().
        write( SIMPLE_OUT, null ).
        assertResult( SIMPLE_IN_WITH_TYPE );
    }

    /*
     * TODO
     * aliases
     */
}
