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
package org.apache.brooklyn.util.yoml.tests;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlTypeFromOtherField;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Objects;

/** Tests that top-level fields can be set at the outer level in yaml. */
public class FieldTypeFromOtherFieldTest {

    @YomlAllFieldsTopLevel
    static abstract class FieldTypeFromOtherAbstract {

        public abstract Object val();
        
        @Override
        public int hashCode() {
            return Objects.hashCode(val());
        }
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FieldTypeFromOtherAbstract)) return false;
            return Objects.equal(val(),  ((FieldTypeFromOtherAbstract)obj).val());
        }
    }
    
    
    @Alias("fto")
    static class FieldTypeFromOther extends FieldTypeFromOtherAbstract {
        
        public FieldTypeFromOther(String typ, Object val) {
            this.typ = typ;
            this.val = val;
        }
        public FieldTypeFromOther() {}

        String typ;
        
        @YomlTypeFromOtherField("typ")
        Object val;
        
        @Override
        public Object val() {
            return val;
        }
    }

    protected FieldTypeFromOther read(String input) {
        YomlTestFixture y = fixture();
        y.read(input, "fto" );
        Asserts.assertInstanceOf(y.lastReadResult, FieldTypeFromOther.class);
        return (FieldTypeFromOther)y.lastReadResult;
    }

    protected YomlTestFixture fixture() {
        return YomlTestFixture.newInstance().addTypeWithAnnotations(FieldTypeFromOther.class);
    }
    
    @Test
    public void testValueFromMap() {
        Assert.assertEquals(read("{ val: { type: int, value: 42 } }").val, 42);
    }
    
    @Test
    public void testTypeUsed() {
        Assert.assertEquals(read("{ typ: int, val: 42 }").val, 42);
    }
    
    @Test
    public void testReadWriteWithType() {
        fixture().reading("{ type: fto, typ: int, val: 42 }").writing(new FieldTypeFromOther("int", 42)).doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testReadWriteWithoutType() {
        fixture().reading("{ type: fto, val: { type: int, value: 42 } }").writing(new FieldTypeFromOther(null, 42)).doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testFailsIfTypeUnknown() {
        try {
            FieldTypeFromOther result = read("{ val: 42 }");
            Asserts.shouldHaveFailedPreviously("Instead got "+result);
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "val");
        }
    }
    
    @Test
    public void testMapSupportedWithType() {
        Assert.assertEquals(read("{ typ: int, val: { type: int, value: 42 } }").val, 42);
    }

    @Alias("fto-type-key-not-real")
    static class FieldTypeFromOtherNotReal extends FieldTypeFromOtherAbstract {
        
        public FieldTypeFromOtherNotReal(Object val) {
            this.val = val;
        }
        public FieldTypeFromOtherNotReal() {}

        @YomlTypeFromOtherField(value="typ", real=false)
        Object val;
        
        @Override
        public Object val() {
            return val;
        }
    }

    @Test
    public void testReadWriteWithTypeInNotRealKey() {
        // in this mode the field is in yaml but not on the object
        YomlTestFixture.newInstance().addTypeWithAnnotations(FieldTypeFromOtherNotReal.class)
            .reading("{ type: fto-type-key-not-real, typ: int, val: 42 }").writing(new FieldTypeFromOtherNotReal(42)).doReadWriteAssertingJsonMatch();
    }

    
    // TODO test w config
    
}
