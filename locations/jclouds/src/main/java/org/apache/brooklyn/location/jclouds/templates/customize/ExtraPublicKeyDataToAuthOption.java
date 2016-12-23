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

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.jclouds.compute.options.TemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExtraPublicKeyDataToAuthOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(ExtraPublicKeyDataToAuthOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        // this is unreliable:
        // * seems now (Aug 2016) to be run *before* the TO.runScript which creates the user,
        // so is installed for the initial login user not the created user
        // * not supported in GCE (it uses it as the login public key, see email to jclouds list, 29 Aug 2015)
        // so only works if you also overrideLoginPrivateKey
        // --
        // for this reason we also inspect these ourselves
        // along with EXTRA_PUBLIC_KEY_URLS_TO_AUTH
        // and install after creation;
        // --
        // we also do it here for legacy reasons though i (alex) can't think of any situations it's needed
        // --
        // also we warn on exceptions in case someone is dumping comments or something else
        try {
            t.authorizePublicKey(v.toString());
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.warn("Error trying jclouds authorizePublicKey; will run later: " + e, e);
        }
    }
}
