package org.apache.brooklyn.container.location.kubernetes;

import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CA_CERT_DATA;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CA_CERT_FILE;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_CERT_DATA;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_CERT_FILE;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_KEY_ALGO;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_KEY_DATA;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_KEY_FILE;
import static org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig.CLIENT_KEY_PASSPHRASE;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

class KubernetesCerts {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesCerts.class);

    public final Optional<String> caCertData;
    public final Optional<String> clientCertData;
    public final Optional<String> clientKeyData;
    public final Optional<String> clientKeyAlgo;
    public final Optional<String> clientKeyPassphrase;
    
    public KubernetesCerts(ConfigBag config) {
        caCertData = getData(CA_CERT_DATA, CA_CERT_FILE, config);
        clientCertData = getData(CLIENT_CERT_DATA, CLIENT_CERT_FILE, config);
        clientKeyData = getData(CLIENT_KEY_DATA, CLIENT_KEY_FILE, config);
        clientKeyAlgo = getNonBlankOptional(CLIENT_KEY_ALGO, config);
        clientKeyPassphrase = getNonBlankOptional(CLIENT_KEY_PASSPHRASE, config);
    }

    protected Optional<String> getData(ConfigKey<String> dataKey, ConfigKey<String> fileKey, ConfigBag config) {
        String data = Strings.isNonBlank(config.get(dataKey)) ? config.get(dataKey).trim() : null;
        String file = config.get(fileKey);
        String fileData = Strings.isNonBlank(file) ? getFileContents(file).trim() : null;
        
        if (Strings.isNonBlank(data) && Strings.isNonBlank(fileData)) {
            if (data.equals(fileData)) {
                LOG.warn("Duplicate (matching) configuration for " + dataKey.getName() + " and " + fileKey.getName() + " (continuing)");
            } else {
                throw new IllegalStateException("Duplicate conflicting configuration for " + dataKey.getName() + " and " + fileKey.getName());
            }
        }
        
        String result = Strings.isNonBlank(data) ? data : (Strings.isNonBlank(fileData) ? fileData : null);
        return Optional.fromNullable(result);
    }

    protected Optional<String> getNonBlankOptional(ConfigKey<? extends String> key, ConfigBag config) {
        String result = config.get(key);
        return Optional.fromNullable(Strings.isNonBlank(result) ? result : null);
    }
    
    protected String getFileContents(String file) {
        return ResourceUtils.create(this).getResourceAsString(file);
    }
}
