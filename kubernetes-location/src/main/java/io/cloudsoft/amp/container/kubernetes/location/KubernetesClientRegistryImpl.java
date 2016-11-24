package io.cloudsoft.amp.container.kubernetes.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.base.Throwables;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesClientRegistryImpl implements KubernetesClientRegistry {

    public static final KubernetesClientRegistryImpl INSTANCE = new KubernetesClientRegistryImpl();

    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        String masterUrl = checkNotNull(conf.get(KubernetesLocationConfig.MASTER_URL), "master url must not be null");

        URL url;
        try {
            url = new URL(masterUrl);
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }

        ConfigBuilder configBuilder = new ConfigBuilder()
                .withMasterUrl(masterUrl)
                .withTrustCerts(false);

        if (url.getProtocol().equals("https")) {
            String caCert = conf.get(KubernetesLocationConfig.CA_CERT);
            if (Strings.isNonBlank(caCert)) configBuilder.withCaCertFile(Os.tidyPath(caCert));

            String clientCert = conf.get(KubernetesLocationConfig.CLIENT_CERT);
            if (Strings.isNonBlank(clientCert)) configBuilder.withClientCertFile(Os.tidyPath(clientCert));

            String clientKey = conf.get(KubernetesLocationConfig.CLIENT_KEY);
            if (Strings.isNonBlank(clientKey)) configBuilder.withClientKeyFile(Os.tidyPath(clientKey));
        }

        String username = conf.get(KubernetesLocationConfig.ACCESS_IDENTITY);
        if (Strings.isNonBlank(username)) configBuilder.withUsername(username);

        String password = conf.get(KubernetesLocationConfig.ACCESS_CREDENTIAL);
        if (Strings.isNonBlank(password)) configBuilder.withPassword(password);

        String token = conf.get(KubernetesLocationConfig.OAUTH_TOKEN);
        if (Strings.isNonBlank(token)) configBuilder.withOauthToken(token);

        return new DefaultKubernetesClient(configBuilder.build());
    }
}
