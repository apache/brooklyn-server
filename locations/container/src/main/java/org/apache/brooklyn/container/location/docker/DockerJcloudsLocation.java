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
package org.apache.brooklyn.container.location.docker;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.container.entity.docker.DockerContainer;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationCustomizer;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.docker.compute.options.DockerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * For provisioning docker containers, using the jclouds-docker integration.
 * <p>
 * This adds special support for default Cloudsoft images. If the image description matches our
 * cloudsoft regexes, then auto-generate a password and pass that in as
 * {@code CLOUDSOFT_ROOT_PASSWORD} when launching the container. That will then be used as the
 * {@link DockerTemplateOptions#getLoginPassword()}.
 * <p>
 * Also, if no image is specified then this will set the default to "cloudsoft/centos:7"
 * (see https://hub.docker.com/r/cloudsoft/centos/).
 */
public class DockerJcloudsLocation extends JcloudsLocation {

    public static final ConfigKey<Boolean> INJECT_LOGIN_CREDENTIAL = ConfigKeys.newBooleanConfigKey(
            "injectLoginCredential",
            "Whether to inject login credentials (if null, will infer from image choice)",
            null);
    public static final ConfigKey<String> DEFAULT_IMAGE_DESCRIPTION_REGEX = ConfigKeys.newStringConfigKey(
            "defaultImageDescriptionRegex",
            "The default image description to use, if no other image preferences are supplied",
            "cloudsoft/centos:7");
    private static final Logger LOG = LoggerFactory.getLogger(DockerJcloudsLocation.class);
    /**
     * The regex for the image descriptions that support us injecting login credentials.
     */
    private static final List<String> IMAGE_DESCRIPTION_REGEXES_REQUIRING_INJECTED_LOGIN_CREDS = ImmutableList.of(
            "cloudsoft/centos.*",
            "cloudsoft/ubuntu.*");

    private static final List<ImageMetadata> DEFAULT_IMAGES = ImmutableList.of(
            new ImageMetadata(OsFamily.CENTOS, "7", "cloudsoft/centos:7"),
            new ImageMetadata(OsFamily.UBUNTU, "14.04", "cloudsoft/ubuntu:14.04"),
            new ImageMetadata(OsFamily.UBUNTU, "16.04", "cloudsoft/ubuntu:16.04"));

    @Override
    protected MachineLocation obtainOnce(ConfigBag setup) throws NoMachinesAvailableException {
        // Use the provider name that jclouds expects; rely on resolver to have validated this.
        setup.configure(JcloudsLocation.CLOUD_PROVIDER, "docker");

        // Inject default image, if absent
        String imageId = setup.get(JcloudsLocation.IMAGE_ID);
        String imageNameRegex = setup.get(JcloudsLocation.IMAGE_NAME_REGEX);
        String imageDescriptionRegex = setup.get(JcloudsLocation.IMAGE_DESCRIPTION_REGEX);
        String defaultImageDescriptionRegex = setup.get(DEFAULT_IMAGE_DESCRIPTION_REGEX);
        OsFamily osFamily = setup.get(OS_FAMILY);
        String osVersionRegex = setup.get(OS_VERSION_REGEX);

        if (Strings.isBlank(imageId) && Strings.isBlank(imageNameRegex) && Strings.isBlank(imageDescriptionRegex)) {
            if (osFamily != null || osVersionRegex != null) {
                for (ImageMetadata imageMetadata : DEFAULT_IMAGES) {
                    if (imageMetadata.matches(osFamily, osVersionRegex)) {
                        String imageDescription = imageMetadata.getImageDescription();
                        LOG.debug("Setting default image regex to {}, for obtain call in {}; removing osFamily={} and osVersionRegex={}",
                                new Object[]{imageDescription, this, osFamily, osVersionRegex});
                        setup.configure(JcloudsLocation.IMAGE_DESCRIPTION_REGEX, imageDescription);
                        setup.configure(OS_FAMILY, null);
                        setup.configure(OS_VERSION_REGEX, null);
                        break;
                    }
                }
            } else if (Strings.isNonBlank(defaultImageDescriptionRegex)) {
                LOG.debug("Setting default image regex to {}, for obtain call in {}", defaultImageDescriptionRegex, this);
                setup.configure(JcloudsLocation.IMAGE_DESCRIPTION_REGEX, defaultImageDescriptionRegex);
            }
        }

        return super.obtainOnce(setup);
    }

    @Override
    public Template buildTemplate(ComputeService computeService, ConfigBag config, Collection<JcloudsLocationCustomizer> customizers) {
        String loginUser = config.get(JcloudsLocation.LOGIN_USER);
        String loginPassword = config.get(JcloudsLocation.LOGIN_USER_PASSWORD);
        String loginKeyFile = config.get(JcloudsLocation.LOGIN_USER_PRIVATE_KEY_FILE);
        String loginKeyData = config.get(JcloudsLocation.LOGIN_USER_PRIVATE_KEY_DATA);

        Template template = super.buildTemplate(computeService, config, customizers);
        DockerTemplateOptions templateOptions = (DockerTemplateOptions) template.getOptions();
        Image image = template.getImage();
        List<String> env = MutableList.copyOf(templateOptions.getEnv());

        // Inject login credentials, if required
        Boolean injectLoginCredentials = config.get(INJECT_LOGIN_CREDENTIAL);
        if (injectLoginCredentials == null) {
            String imageDescription = image.getDescription();
            for (String regex : IMAGE_DESCRIPTION_REGEXES_REQUIRING_INJECTED_LOGIN_CREDS) {
                if (imageDescription != null && imageDescription.matches(regex)) {
                    injectLoginCredentials = true;
                    break;
                }
            }
        }
        if (Strings.isBlank(loginUser) && Strings.isBlank(loginPassword) && Strings.isBlank(loginKeyFile) && Strings.isBlank(loginKeyData)) {
            if (Boolean.TRUE.equals(injectLoginCredentials)) {
                loginUser = "root";
                loginPassword = Identifiers.makeRandomPassword(12);
                templateOptions.overrideLoginUser(loginUser);
                templateOptions.overrideLoginPassword(loginPassword);

                env.add("CLOUDSOFT_ROOT_PASSWORD=" + loginPassword);
            }
        }

        Entity context = validateCallerContext(config);
        Map<String, Object> containerEnv = MutableMap.copyOf(context.config().get(DockerContainer.CONTAINER_ENVIRONMENT));
        for (Map.Entry<String, String> entry : Maps.transformValues(containerEnv, Functions.toStringFunction()).entrySet()) {
            env.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
        }
        templateOptions.env(env);

        return template;
    }

    private Entity validateCallerContext(ConfigBag setup) {
        // Lookup entity flags
        Object callerContext = setup.get(LocationConfigKeys.CALLER_CONTEXT);
        if (callerContext == null || !(callerContext instanceof Entity)) {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
        return (Entity) callerContext;
    }

    private static class ImageMetadata {
        private final OsFamily osFamily;
        private final String osVersion;
        private final String imageDescription;

        public ImageMetadata(OsFamily osFamily, String osVersion, String imageDescription) {
            this.osFamily = checkNotNull(osFamily, "osFamily");
            this.osVersion = checkNotNull(osVersion, "osVersion");
            this.imageDescription = checkNotNull(imageDescription, "imageDescription");
        }

        public boolean matches(@Nullable OsFamily osFamily, @Nullable String osVersionRegex) {
            if (osFamily != null && osFamily != this.osFamily) return false;
            if (osVersionRegex != null && !osVersion.matches(osVersionRegex)) return false;
            return true;
        }

        public String getImageDescription() {
            return imageDescription;
        }
    }
}
