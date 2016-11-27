package io.cloudsoft.amp.containerservice.kubernetes.location;

import org.apache.brooklyn.launcher.blueprints.AbstractBlueprintTest;
import org.testng.annotations.Test;

public class SimpleBlueprintsLiveTest extends AbstractBlueprintTest {

    // TODO The yaml hard-codes a k8s endpoint URL that is no longer running
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testSimpleServer() throws Exception {
        runTest("blueprints/simple-server-on-kubernetes.yaml");
    }

    // TODO The yaml hard-codes a k8s endpoint URL that is no longer running
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testVanillaSoftwareProcess() throws Exception {
        runTest("blueprints/vanilla-software-process-on-kubernetes.yaml");
    }

    // TODO The yaml hard-codes a k8s endpoint URL that is no longer running
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testTomcatDockerImage() throws Exception {
        runTest("blueprints/tomcat-docker-image-on-kubernetes.yaml");
    }

    // TODO The yaml hard-codes a k8s endpoint URL that is no longer running
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testWordpressDockerImage() throws Exception {
        runTest("blueprints/mysql_wordpress-docker-images-on-kubernetes.yaml");
    }

    // TODO The yaml hard-codes a k8s endpoint URL that is no longer running
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testIronRunnerDockerImage() throws Exception {
        runTest("blueprints/iron_runner-docker-image-on-kubernetes.yaml");
    }
}
