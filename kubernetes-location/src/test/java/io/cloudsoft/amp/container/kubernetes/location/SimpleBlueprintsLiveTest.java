package io.cloudsoft.amp.container.kubernetes.location;

import org.apache.brooklyn.launcher.blueprints.AbstractBlueprintTest;
import org.testng.annotations.Test;

public class SimpleBlueprintsLiveTest extends AbstractBlueprintTest {

    @Test(groups={"Live"})
    public void testSimpleServer() throws Exception {
        runTest("blueprints/simple-server-on-kubernetes.yaml");
    }

    @Test(groups={"Live"})
    public void testVanillaSoftwareProcess() throws Exception {
        runTest("blueprints/vanilla-software-process-on-kubernetes.yaml");
    }

    @Test(groups={"Live"})
    public void testTomcatDockerImage() throws Exception {
        runTest("blueprints/tomcat-docker-image-on-kubernetes.yaml");
    }

    @Test(groups={"Live"})
    public void testWordpressDockerImage() throws Exception {
        runTest("blueprints/mysql_wordpress-docker-images-on-kubernetes.yaml");
    }

    @Test(groups={"Live"}, enabled = false)
    public void testIronRunnerDockerImage() throws Exception {
        runTest("blueprints/iron_runner-docker-image-on-kubernetes.yaml");
    }
}
