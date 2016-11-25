package io.cloudsoft.amp.container.kubernetes.location;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ImageChooserTest {

    private ImageChooser chooser;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() {
        chooser = new ImageChooser();
    }
    
    @Test
    public void testDefault() throws Exception {
        assertEquals(chooser.chooseImage((String)null, null).get(), "cloudsoft/centos:7");
    }

    @Test
    public void testCentos() throws Exception {
        assertEquals(chooser.chooseImage("cEnToS", null).get(), "cloudsoft/centos:7");
    }

    @Test
    public void testCentos7() throws Exception {
        assertEquals(chooser.chooseImage("cEnToS", "7").get(), "cloudsoft/centos:7");
    }

    @Test
    public void testUbnutu() throws Exception {
        assertEquals(chooser.chooseImage("uBuNtU", null).get(), "cloudsoft/ubuntu:14.04");
    }

    @Test
    public void testUbnutu14() throws Exception {
        assertEquals(chooser.chooseImage("uBuNtU", "14.*").get(), "cloudsoft/ubuntu:14.04");
    }

    @Test
    public void testUbnutu16() throws Exception {
        assertEquals(chooser.chooseImage("uBuNtU", "16.*").get(), "cloudsoft/ubuntu:16.04");
    }

    @Test
    public void testAbsentForCentos6() throws Exception {
        assertFalse(chooser.chooseImage("cEnToS", "6").isPresent());
    }

    @Test
    public void testAbsentForUbuntu15() throws Exception {
        assertFalse(chooser.chooseImage("uBuNtU", "15").isPresent());
    }

    @Test
    public void testAbsentForDebian() throws Exception {
        assertFalse(chooser.chooseImage("debian", null).isPresent());
    }

    @Test
    public void testAbsentForWrongOsFamily() throws Exception {
        assertFalse(chooser.chooseImage("weirdOsFamily", null).isPresent());
    }
}
