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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.softlayer.compute.options.SoftLayerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UserMetadataStringOption implements TemplateOptionCustomizer {
    private static final Logger LOG = LoggerFactory.getLogger(UserMetadataStringOption.class);

    public void apply(TemplateOptions t, ConfigBag props, Object v) {
        if (t instanceof EC2TemplateOptions) {
            // See AWS docs: http://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/UsingConfig_WinAMI.html#user-data-execution
            if (v == null) return;
            String data = v.toString();
            if (!(data.startsWith("<script>") || data.startsWith("<powershell>"))) {
                data = "<script> " + data + " </script>";
            }
            ((EC2TemplateOptions) t).userData(data.getBytes());
        } else if (t instanceof SoftLayerTemplateOptions) {
            ((SoftLayerTemplateOptions) t).userData(Strings.toString(v));
        } else {
            // Try reflection: userData(String), or guestCustomizationScript(String);
            // the latter is used by vCloud Director.
            Class<? extends TemplateOptions> clazz = t.getClass();
            Method userDataMethod = null;
            try {
                userDataMethod = clazz.getMethod("userData", String.class);
            } catch (SecurityException e) {
                LOG.info("Problem reflectively inspecting methods of " + t.getClass() + " for setting userData", e);
            } catch (NoSuchMethodException e) {
                try {
                    // For vCloud Director
                    userDataMethod = clazz.getMethod("guestCustomizationScript", String.class);
                } catch (NoSuchMethodException e2) {
                    // expected on various other clouds
                }
            }
            if (userDataMethod != null) {
                try {
                    userDataMethod.invoke(t, Strings.toString(v));
                } catch (InvocationTargetException e) {
                    LOG.info("Problem invoking " + userDataMethod.getName() + " of " + t.getClass() + ", for setting userData (rethrowing)", e);
                    throw Exceptions.propagate(e);
                } catch (IllegalAccessException e) {
                    LOG.debug("Unable to reflectively invoke " + userDataMethod.getName() + " of " + t.getClass() + ", for setting userData (rethrowing)", e);
                    throw Exceptions.propagate(e);
                }
            } else {
                LOG.info("ignoring userDataString({}) in VM creation because not supported for cloud/type ({})", v, t.getClass());
            }
        }
    }
}
