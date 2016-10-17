package io.cloudsoft.amp.container.kubernetes.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.os.Os;

import com.google.common.base.Throwables;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesClientRegistryImpl implements KubernetesClientRegistry {

    public static final KubernetesClientRegistryImpl INSTANCE = new KubernetesClientRegistryImpl();

    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        String masterUrl = checkNotNull(conf.get(KubernetesLocationConfig.MASTER_URL), "master url must not be null");
        String username = conf.get(KubernetesLocationConfig.ACCESS_IDENTITY);
        String password = conf.get(KubernetesLocationConfig.ACCESS_CREDENTIAL);

        ConfigBuilder configBuilder = new ConfigBuilder()
                .withMasterUrl(masterUrl)
                .withTrustCerts(false);

        URL url;
        try {
            url = new URL(masterUrl);
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }

        if (url.getProtocol().equals("https")) {
            String caCert = checkNotNull(conf.get(KubernetesLocationConfig.CA_CERT), "caCertl must not be null");
            String clientCert = checkNotNull(conf.get(KubernetesLocationConfig.CLIENT_CERT), "clientCert must not be null");
            String clientKey = checkNotNull(conf.get(KubernetesLocationConfig.CLIENT_KEY), "clientKey must not be null");

            configBuilder.withCaCertFile(Os.tidyPath(caCert))
                    .withClientCertFile(Os.tidyPath(clientCert))
                    .withClientKeyFile(Os.tidyPath(clientKey));
        } else if(username != null && password != null) {
            configBuilder.withUsername(username).withPassword(password);
        }

        return new DefaultKubernetesClient(configBuilder.build());
    }

}
