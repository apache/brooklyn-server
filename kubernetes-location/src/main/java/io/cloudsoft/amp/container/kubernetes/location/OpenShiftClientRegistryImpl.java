package io.cloudsoft.amp.container.kubernetes.location;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.os.Os;

import com.google.common.base.Throwables;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftConfigBuilder;

public class OpenShiftClientRegistryImpl implements KubernetesClientRegistry {

    public static final OpenShiftClientRegistryImpl INSTANCE = new OpenShiftClientRegistryImpl();

    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        String masterUrl = checkNotNull(conf.get(KubernetesLocationConfig.MASTER_URL), "master url must not be null");
        String username = conf.get(KubernetesLocationConfig.ACCESS_IDENTITY);
        String password = conf.get(KubernetesLocationConfig.ACCESS_CREDENTIAL);
        String openshiftUrl = checkNotNull(conf.get(OpenShiftLocationConfig.OPENSHIFT_URL), "openshift url must not be null");

        OpenShiftConfigBuilder configBuilder = new OpenShiftConfigBuilder()
                .withOpenShiftUrl(openshiftUrl)
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

        return new DefaultOpenShiftClient(configBuilder.build());
    }

}
