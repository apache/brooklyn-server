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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PerverseSerializationTest implements MapperTestFixture {

    private static class BeanWithFieldCalledType {
        String type;
    }

    @Test
    public void testFieldCalledTypeMakesInvalidJson() throws Exception {
        BeanWithFieldCalledType a = new BeanWithFieldCalledType();
        a.type = "not my type";
        Assert.assertEquals(ser(a),
                "{\"type\":\""+ BeanWithFieldCalledType.class.getName()+"\",\"type\":\"not my type\"}");
    }

    private final static class BeanWithFieldCalledTypeHolder {
        BeanWithFieldCalledType bean;
    }

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(null);
    }

    @Test
    public void testFieldCalledTypeOkayIfTypeConcrete() throws Exception {
        BeanWithFieldCalledType a = new BeanWithFieldCalledType();
        a.type = "not my type";
        BeanWithFieldCalledTypeHolder b = new BeanWithFieldCalledTypeHolder();
        b.bean = a;
        String expected = "{\"type\":\"" + BeanWithFieldCalledTypeHolder.class.getName() + "\"," +
                "\"bean\":{\"type\":\"not my type\"}" +
                "}";
        Assert.assertEquals(ser(b), expected);

        BeanWithFieldCalledTypeHolder b2 = deser(expected);
        Assert.assertEquals(b2.bean.type, "not my type");
    }


}
