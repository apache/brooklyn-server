package io.cloudsoft.amp.containerservice.kubernetes.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import javax.annotation.Nullable;

import org.jclouds.compute.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class ImageChooser {

    private static final Logger LOG = LoggerFactory.getLogger(ImageChooser.class);

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

    private static final List<ImageMetadata> DEFAULT_IMAGES = ImmutableList.of(
            new ImageMetadata(OsFamily.CENTOS, "7", "cloudsoft/centos:7"),
            new ImageMetadata(OsFamily.UBUNTU, "14.04", "cloudsoft/ubuntu:14.04"),
            new ImageMetadata(OsFamily.UBUNTU, "16.04", "cloudsoft/ubuntu:16.04"));
    
    private final List<ImageMetadata> images;

    public ImageChooser() {
        this.images = DEFAULT_IMAGES;
    }
    
    public ImageChooser(List<? extends ImageMetadata> images) {
        this.images = ImmutableList.copyOf(images);
    }

    public Optional<String> chooseImage(String osFamily, String osVersionRegex) {
        return chooseImage((osFamily == null ? (OsFamily)null : OsFamily.fromValue(osFamily)), osVersionRegex);
    }
    
    public Optional<String> chooseImage(OsFamily osFamily, String osVersionRegex) {
        for (ImageMetadata imageMetadata : images) {
            if (imageMetadata.matches(osFamily, osVersionRegex)) {
                String imageName = imageMetadata.getImageName();
                LOG.debug("Choosing container image {}, for osFamily={} and osVersionRegex={}", new Object[] {imageName, osFamily, osVersionRegex});
                return Optional.of(imageName);
            }
        }
        return Optional.absent();
    }
}
