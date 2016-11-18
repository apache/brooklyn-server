package io.cloudsoft.amp.container.openshift.location;

import org.apache.brooklyn.launcher.blueprints.AbstractBlueprintTest;
import org.testng.annotations.Test;

public class SimpleBlueprintsLiveTest extends AbstractBlueprintTest {

    // TODO These blueprints won't work because they don't have enough config to make the location
    // viable - e.g. no cert files, etc.

    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testSimpleServer() throws Exception {
        runTest("blueprints/simple-server-on-openshift.yaml");
    }

    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testVanillaSoftwareProcess() throws Exception {
        runTest("blueprints/vanilla-software-process-on-openshift.yaml");
    }

    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testTomcatDockerImage() throws Exception {
        runTest("blueprints/tomcat-docker-image-on-openshift.yaml");
    }

    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testWordpressDockerImage() throws Exception {
        runTest("blueprints/mysql_wordpress-docker-images-on-openshift.yaml");
    }

    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testIronRunnerDockerImage() throws Exception {
        runTest("blueprints/iron_runner-docker-image-on-openshift.yaml");
    }
}
