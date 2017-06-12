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
package org.apache.brooklyn.container.location.kubernetes;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.jclouds.compute.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImageChooser {

    private static final Logger LOG = LoggerFactory.getLogger(ImageChooser.class);
    private static final List<ImageMetadata> DEFAULT_IMAGES = ImmutableList.of(
            new ImageMetadata(OsFamily.CENTOS, "7", "brooklyncentral/centos:7"),
            new ImageMetadata(OsFamily.UBUNTU, "14.04", "brooklyncentral/ubuntu:14.04"),
            new ImageMetadata(OsFamily.UBUNTU, "16.04", "brooklyncentral/ubuntu:16.04"));
    private final List<ImageMetadata> images;

    public ImageChooser() {
        this.images = DEFAULT_IMAGES;
    }

    public ImageChooser(List<? extends ImageMetadata> images) {
        this.images = ImmutableList.copyOf(images);
    }

    public Optional<String> chooseImage(String osFamily, String osVersionRegex) {
        return chooseImage((osFamily == null ? (OsFamily) null : OsFamily.fromValue(osFamily)), osVersionRegex);
    }

    public Optional<String> chooseImage(OsFamily osFamily, String osVersionRegex) {
        for (ImageMetadata imageMetadata : images) {
            if (imageMetadata.matches(osFamily, osVersionRegex)) {
                String imageName = imageMetadata.getImageName();
                LOG.debug("Choosing container image {}, for osFamily={} and osVersionRegex={}", new Object[]{imageName, osFamily, osVersionRegex});
                return Optional.of(imageName);
            }
        }
        return Optional.absent();
    }

    public static class ImageMetadata {
        private final OsFamily osFamily;
        private final String osVersion;
        private final String imageName;

        public ImageMetadata(OsFamily osFamily, String osVersion, String imageName) {
            this.osFamily = checkNotNull(osFamily, "osFamily");
            this.osVersion = checkNotNull(osVersion, "osVersion");
            this.imageName = checkNotNull(imageName, "imageName");
        }

        public boolean matches(@Nullable OsFamily osFamily, @Nullable String osVersionRegex) {
            if (osFamily != null && osFamily != this.osFamily) return false;
            if (osVersionRegex != null && !osVersion.matches(osVersionRegex)) return false;
            return true;
        }

        public String getImageName() {
            return imageName;
        }
    }
}
