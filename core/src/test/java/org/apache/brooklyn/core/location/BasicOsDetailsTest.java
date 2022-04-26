package org.apache.brooklyn.core.location;

import org.apache.brooklyn.api.location.OsDetails;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class BasicOsDetailsTest {

    @Test
    public void testMacIntel2019() {
        OsDetails testMacOsIntel2019 = new BasicOsDetails("Mac OS X","x86_64","10.16");
        assertTrue(testMacOsIntel2019.isMac());
        assertFalse(testMacOsIntel2019.isArm());
        assertTrue(testMacOsIntel2019.is64bit());
    }

    @Test
    public void testMacM12021() {
        OsDetails testMacOsIntel2019 = new BasicOsDetails("macOS","aarch64","10.19");
        assertTrue(testMacOsIntel2019.isMac());
        assertTrue(testMacOsIntel2019.isArm());
        assertTrue(testMacOsIntel2019.is64bit());
    }

    @Test
    public void testMyOs() {
        OsDetails myOs = new BasicOsDetails("myname", "myarch", "myversion");
        assertFalse(myOs.isMac());
        assertFalse(myOs.isArm());
        assertFalse(myOs.is64bit());
    }

    @Test
    public void testMyOs64() {
        OsDetails myOs64 = new BasicOsDetails("myname", "myarch64", "myversion");
        assertFalse(myOs64.isMac());
        assertFalse(myOs64.isArm());
        assertTrue(myOs64.is64bit());
    }
}
