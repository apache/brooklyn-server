/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.rest.util;

import org.apache.brooklyn.rest.apidoc.RestApiResourceScanner;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;

import io.swagger.config.ScannerFactory;

public class ScannerInjectHelper {
    public void setServer(JAXRSServerFactoryBean server) {
        ScannerFactory.setScanner(new RestApiResourceScanner(server.getResourceClasses()));
    }
}
