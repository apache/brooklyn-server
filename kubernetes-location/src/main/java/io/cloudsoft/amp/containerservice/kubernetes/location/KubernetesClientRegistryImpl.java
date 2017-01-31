package io.cloudsoft.amp.containerservice.kubernetes.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;

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
            KubernetesCerts certs = new KubernetesCerts(conf);
            if (certs.caCertData.isPresent()) configBuilder.withCaCertData(toBase64Encoding(certs.caCertData.get()));
            if (certs.clientCertData.isPresent()) configBuilder.withClientCertData(toBase64Encoding(certs.clientCertData.get()));
            if (certs.clientKeyData.isPresent()) configBuilder.withClientKeyData(toBase64Encoding(certs.clientKeyData.get()));
            if (certs.clientKeyAlgo.isPresent()) configBuilder.withClientKeyAlgo(certs.clientKeyAlgo.get());
            if (certs.clientKeyPassphrase.isPresent()) configBuilder.withClientKeyPassphrase(certs.clientKeyPassphrase.get());
            // TODO configBuilder.withTrustCerts(true);
        }

        String username = conf.get(KubernetesLocationConfig.ACCESS_IDENTITY);
        if (Strings.isNonBlank(username)) configBuilder.withUsername(username);

        String password = conf.get(KubernetesLocationConfig.ACCESS_CREDENTIAL);
        if (Strings.isNonBlank(password)) configBuilder.withPassword(password);

        String token = conf.get(KubernetesLocationConfig.OAUTH_TOKEN);
        if (Strings.isNonBlank(token)) configBuilder.withOauthToken(token);

        Duration clientTimeout = conf.get(KubernetesLocationConfig.CLIENT_TIMEOUT);
        if (clientTimeout.isPositive()) {
            configBuilder.withConnectionTimeout((int) clientTimeout.toMilliseconds());
            configBuilder.withRequestTimeout((int) clientTimeout.toMilliseconds());
        }
        Duration actionTimeout = conf.get(KubernetesLocationConfig.ACTION_TIMEOUT);
        if (actionTimeout.isPositive()) {
            configBuilder.withRollingTimeout(actionTimeout.toMilliseconds());
            configBuilder.withScaleTimeout(actionTimeout.toMilliseconds());
        }

        return new DefaultKubernetesClient(configBuilder.build());
    }

    private String toBase64Encoding(String val) {
        return BaseEncoding.base64().encode(val.getBytes());
    }
}
