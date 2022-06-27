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
package org.apache.brooklyn.util.core.xstream;

import java.util.function.BiPredicate;
import org.apache.brooklyn.test.Asserts;
import org.testng.Assert;

import com.thoughtworks.xstream.XStream;

public class ConverterTestFixture {

    protected XStream cachedXstream = null;

    protected XStream newXstream() {
        XStream xstream = new XStream();
        XmlSerializer.allowAllTypes(xstream);
        registerConverters(xstream);
        return xstream;
    }

    protected void useNewCachedXstream() {
        cachedXstream = newXstream();
    }

    protected Object assertX(Object obj, String fmt, String ...others) {
        return assertX(obj, (a,b)->{ Asserts.assertEquals(a,b); return true; }, fmt, others);
    }

    protected <T> T assertX(T obj, BiPredicate<T,T> equals, String fmt, String ...others) {
        XStream xstream = cachedXstream!=null ? cachedXstream : newXstream();

        String s1 = xstream.toXML(obj);
        Assert.assertEquals(s1, fmt);
        T out1 = (T) xstream.fromXML(s1);
        if (!equals.test(out1, obj)) Asserts.fail("Objects not equal:\n"+out1+"\n---\n"+obj);
        for (String other: others) {
            try {
                T outO = (T)xstream.fromXML(other);
                if (!equals.test(outO, obj)) Asserts.fail("Objects not equal:\n"+outO+"\n---\n"+obj);
            } catch (Throwable e) {
                Assert.fail("Expected deserialization fails or produces non-equal object:\n"+other, e);
            }
        }
        return out1;
    }

    protected Object assertX(Object obj, String fmt) {
        XStream xstream = cachedXstream!=null ? cachedXstream : newXstream();

        String s1 = xstream.toXML(obj);
        Assert.assertEquals(s1, fmt);
        Object out = xstream.fromXML(s1);
        Assert.assertEquals(out, obj);
        return out;
    }

    protected void registerConverters(XStream xstream) {
    }

}
