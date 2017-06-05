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
package org.apache.brooklyn.util.osgi;

/**
 * Many OSGi tests require OSGi bundles (of course). Test bundles have been collected here
 * for convenience and clarity. Available bundles (on the classpath, with source code
 * either embedded or in /src/dependencies) are described by the constants in this class.
 * <p>
 * Some of these bundles are also used in REST API tests, as that stretches catalog further
 * (using CAMP) and that is one area where OSGi is heavily used. 
 */
public interface OsgiTestResources {

    /**
     * brooklyn-osgi-test-a_0.1.0 -
     * defines TestA which has a "times" method and a static multiplier field;
     * we set the multiplier to determine when we are sharing versions and when not
     */
    public static final String BROOKLYN_OSGI_TEST_A_0_1_0_PATH = "/brooklyn/osgi/brooklyn-osgi-test-a_0.1.0.jar";

    /**
     * brooklyn-test-osgi-entities (v 0.1.0) -
     * defines an entity and an application, to confirm it can be read and used by brooklyn
     */
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FINAL_PART = "brooklyn-test-osgi-entities";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_VERSION = "0.1.0";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FULL =
        "org.apache.brooklyn.test.resources.osgi."+BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FINAL_PART;
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_PATH = "/brooklyn/osgi/brooklyn-test-osgi-entities.jar";
    public static final String BROOKLYN_TEST_OSGI_MORE_ENTITIES_0_1_0_PATH = "/brooklyn/osgi/brooklyn-test-osgi-more-entities_0.1.0.jar";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_MESSAGE_RESOURCE = "/org/apache/brooklyn/test/osgi/resources/message.txt";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_APPLICATION = "org.apache.brooklyn.test.osgi.entities.SimpleApplication";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY = "org.apache.brooklyn.test.osgi.entities.SimpleEntity";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_POLICY = "org.apache.brooklyn.test.osgi.entities.SimplePolicy";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENRICHER = "org.apache.brooklyn.test.osgi.entities.SimpleEnricher";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_EFFECTOR = "org.apache.brooklyn.test.osgi.entities.SimpleEffectorInitializer";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_OBJECT = "org.apache.brooklyn.test.osgi.entities.SimpleObject";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY_CONFIG_NAME = "simple.config";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY_SENSOR_NAME = "simple.sensor";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_UNPREFIXED_DUMMY_EXTERNAL_CONFIG_SUPPLIER = "com.example.brooklyn.test.osgi.UnprefixedDummyExternalConfigSupplier";

    /**
     * brooklyn-test-com-example-osgi-entities (v 0.1.0) -
     * defines an entity and an application, to confirm it can be read and used by brooklyn.
     * Uses a different symbolic name than the "org.apache.brooklyn" prefix, to test that it's
     * not just "white-listing" of such bundles that allows tests to pass.
     */
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FINAL_PART = "brooklyn-test-osgi-com-example-entities";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL =
        "com.example.brooklyn.test.resources.osgi."+BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FINAL_PART;
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH = "/brooklyn/osgi/brooklyn-test-osgi-com-example-entities.jar";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_URL = "classpath:"+BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH;
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_VERSION = "0.1.0";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ICON_PATH = "/com/example/brooklyn/test/osgi/entities/icon.gif";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_APPLICATION = "com.example.brooklyn.test.osgi.entities.SimpleApplication";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ENTITY = "com.example.brooklyn.test.osgi.entities.SimpleEntity";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_POLICY = "com.example.brooklyn.test.osgi.entities.SimplePolicy";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_LOCATION = "com.example.brooklyn.test.osgi.entities.SimpleLocation";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_EFFECTOR = "com.example.brooklyn.test.osgi.entities.SimpleEffectorInitializer";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_OBJECT = "com.example.brooklyn.test.osgi.entities.SimpleObject";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ENTITY_CONFIG_NAME = "simple.config";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ENTITY_SENSOR_NAME = "simple.sensor";
    public static final String BROOKLYN_TEST_OSGI_ENTITIES_PREFIXED_DUMMY_EXTERNAL_CONFIG_SUPPLIER = "com.example.brooklyn.test.osgi.PrefixedDummyExternalConfigSupplier";

    /**
     * brooklyn-test-osgi-more-entities_0.1.0 -
     * another bundle with a minimal sayHi effector, used to test versioning and dependencies
     * (this one has no dependencies, but see also {@value #BROOKLYN_TEST_MORE_ENTITIES_V2_PATH})
     */
    public static final String BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FINAL_PART = "brooklyn-test-osgi-more-entities";
    public static final String BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL = 
        "org.apache.brooklyn.test.resources.osgi."+BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FINAL_PART;
    public static final String BROOKLYN_TEST_MORE_ENTITIES_V1_PATH = "/brooklyn/osgi/" + BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FINAL_PART + "_0.1.0.jar";
    public static final String BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY = "org.apache.brooklyn.test.osgi.entities.more.MoreEntity";
    
    /**
     * brooklyn-test-osgi-more-entities_0.2.0 -
     * similar to {@link #BROOKLYN_TEST_MORE_ENTITIES_V1_PATH} but saying "HI NAME" rather than "Hi NAME",
     * and declaring an explicit dependency on SimplePolicy from {@link #BROOKLYN_TEST_OSGI_ENTITIES_PATH}
     */
    public static final String BROOKLYN_TEST_MORE_ENTITIES_V2_PATH = "/brooklyn/osgi/" + BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FINAL_PART + "_0.2.0.jar";
    
    /**
     * bundle with identical metadata (same symbolic name and version -- hence being an evil twin) 
     * as {@link #BROOKLYN_TEST_MORE_ENTITIES_V2_PATH},
     * but slightly different behaviour -- saying "HO NAME" -- in order to make sure we can differentiate two two
     * at runtime.
     */
    public static final String BROOKLYN_TEST_MORE_ENTITIES_V2_EVIL_TWIN_PATH = "/brooklyn/osgi/" + BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FINAL_PART + "_evil-twin_0.2.0.jar";


    public static final String BROOKLYN_TEST_OSGI_ENTITIES_URL = "classpath:"+OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH;

    public static final String BROOKLYN_TEST_MORE_ENTITIES_V1_URL = "classpath:"+BROOKLYN_TEST_MORE_ENTITIES_V1_PATH;
    public static final String BROOKLYN_TEST_MORE_ENTITIES_V2_URL = "classpath:"+BROOKLYN_TEST_MORE_ENTITIES_V2_PATH;
    public static final String BROOKLYN_TEST_MORE_ENTITIES_V2_EVIL_TWIN_URL = "classpath:"+BROOKLYN_TEST_MORE_ENTITIES_V2_EVIL_TWIN_PATH;
    
    public static final String TEST_VERSION = "0.1.0";

}
