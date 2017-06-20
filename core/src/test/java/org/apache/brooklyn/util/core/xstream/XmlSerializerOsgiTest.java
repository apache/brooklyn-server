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

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.brooklyn.core.mgmt.classloading.ClassLoaderFromStackOfBrooklynClassLoadingContext;
import org.apache.brooklyn.util.core.osgi.OsgiTestBase;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class XmlSerializerOsgiTest extends OsgiTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(XmlSerializerOsgiTest.class);

    protected XmlSerializer<Object> serializer;
    
    /** Simple osgi-prefix-aware class loader. In the real world this function is done by OsgiManager. */
    public static class OsgiPrefixStrippingClassLoader extends ClassLoader {
        private final Framework framework;

        public OsgiPrefixStrippingClassLoader(Framework framework, ClassLoader parent) { 
            super(parent);
            this.framework = framework;
        }
        
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            int separator = name.indexOf(":");
            if (separator>=0) {
                Bundle bundle = Osgis.bundleFinder(framework).symbolicName(name.substring(0, separator)).find().get();
                return bundle.loadClass(name.substring(separator+1));
            } else {
                return super.findClass(name);
            }
        }
        
        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            int separator = name.indexOf(":");
            if (separator>=0) {
                Bundle bundle = Osgis.bundleFinder(framework).symbolicName(name.substring(0, separator)).find().get();
                return bundle.loadClass(name.substring(separator+1));
            } else {
                return super.loadClass(name, resolve);
            }
        }
    }
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        serializer = new XmlSerializer<Object>(
            new OsgiPrefixStrippingClassLoader(framework, getClass().getClassLoader()), 
            ImmutableMap.<String, String>of());
    }
    
    @Test
    public void testPrefixesOnClassAndInstance() throws Exception {
        Bundle bundle = installFromClasspath(BROOKLYN_OSGI_TEST_A_0_1_0_PATH);
        
        final String TYPE = "brooklyn.test.osgi.TestA";
        final String FQTN = OsgiTestResources.BROOKLYN_OSGI_TEST_A_SYMBOLIC_NAME + ":" + TYPE;
        
        Class<?> aClass = bundle.loadClass(TYPE);
        String rc = assertSerializeEqualsAndCanDeserialize(aClass, "<java-class>"+FQTN+"</java-class>");
        
        Object aInst = aClass.newInstance();
        String ri = assertSerializeEqualsAndCanDeserialize(aInst, "<"+FQTN+"/>");
        
        List<Object> l = Arrays.asList(aClass, aInst);
        assertSerializeEqualsAndCanDeserialize(l, "<java.util.Arrays_-ArrayList><a>"+rc+ri+"</a></java.util.Arrays_-ArrayList>");
    }

    protected String assertSerializeEqualsAndCanDeserialize(Object val, String expected) throws Exception {
        String xml = serializer.toString(val);
        assertEquals(Strings.replaceAllRegex(xml, "\\s+", ""), Strings.replaceAllRegex(expected, "\\s+", ""));
        Object result = serializer.fromString(xml);
        LOG.debug("val="+val+"'; xml="+xml+"; result="+result);
        assertEquals(result.getClass(), val.getClass());
        return expected;
    }
}
