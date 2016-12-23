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

package org.apache.brooklyn.location.jclouds.templates.customize;

import java.io.File;
import java.io.IOException;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

class LoginUserPrivateKeyFileOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(LoginUserPrivateKeyFileOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        if (v != null) {
            String privateKeyFileName = v.toString();
            String privateKey;
            try {
                privateKey = Files.toString(new File(Os.tidyPath(privateKeyFileName)), Charsets.UTF_8);
            } catch (IOException e) {
                LOG.error(privateKeyFileName + "not found", e);
                throw Exceptions.propagate(e);
            }
            t.overrideLoginPrivateKey(privateKey);
        }
    }
}
