/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.core.typereg;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.VersionRangedName;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.BrooklynVersionSyntax;
import org.mockito.Mockito;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;
import org.osgi.framework.VersionRange;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public class BundleUpgradeParserTest {

    private static final String DEFAULT_WILDCARD_NAME = "WILDCARD-NAME";
    private static final String DEFAULT_WILDCARD_VERSION = "0-WILDCARD_VERSION";
    private static final String DEFAULT_WILDCARD_VERSION_RANGE = "[0,"+DEFAULT_WILDCARD_VERSION+")";
    private static final String DEFAULT_TARGET_VERSION = "0-DEFAULT-TARGET";
    
    private VersionRange from0lessThan1 = new VersionRange('[', Version.valueOf("0"), Version.valueOf("1.0.0"), ')');
    private VersionRange from0lessThan1_2_3 = new VersionRange('[', Version.valueOf("0"), Version.valueOf("1.2.3"), ')');
    private VersionRange exactly0dot1 = new VersionRange('[', Version.valueOf("0.1.0"), Version.valueOf("0.1.0"), ']');
    private VersionRangedName fooFrom0lessThan1 = new VersionRangedName("foo", from0lessThan1);
    private VersionRangedName barFrom0lessThan1 = new VersionRangedName("bar", from0lessThan1);

    @Test
    public void testVersionRangedName() throws Exception {
        assertEquals(VersionRangedName.fromString("foo:0.1.0", true).toOsgiString(), "foo:0.1.0");
        assertEquals(VersionRangedName.fromString("foo:0.1.0", false).toOsgiString(), "foo:[0.1.0,0.1.0]");
        assertEquals(VersionRangedName.fromString("foo:[0,1)", false).toOsgiString(), "foo:[0.0.0,1.0.0)");
        
        assertVersionRangedNameFails("foo", "'foo' must be of 'name:versionRange' syntax");
        assertVersionRangedNameFails("foo:bar:0.1.0", "has too many parts");
        assertVersionRangedNameFails("", "Must not be blank");
        assertVersionRangedNameFails(null, "Must not be blank");
    }
    
    @Test
    public void testVersionRangesWithSnapshots() throws Exception {
        VersionRange from0lessThan1 = VersionRangedName.fromString("foo:[0,1)", false).getOsgiVersionRange();
        assertTrue(from0lessThan1.includes(Version.valueOf("0.1.0.SNAPSHOT")));
        assertTrue(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("0.1.0-SNAPSHOT"))));
        assertTrue(from0lessThan1.includes(Version.valueOf("0.0.0.SNAPSHOT")));
        assertTrue(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("0.0.0-SNAPSHOT"))));
        assertFalse(from0lessThan1.includes(Version.valueOf("1.0.0.SNAPSHOT")));
        assertFalse(from0lessThan1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("1.0.0-SNAPSHOT"))));
        
        VersionRange from1 = VersionRangedName.fromString("foo:[1,9999)", false).getOsgiVersionRange();
        assertTrue(from1.includes(Version.valueOf("1.0.0.SNAPSHOT")));
        assertTrue(from1.includes(Version.valueOf(BrooklynVersionSyntax.toValidOsgiVersion("1.SNAPSHOT"))));
        assertFalse(from1.includes(Version.valueOf("0.0.0.SNAPSHOT")));
        assertFalse(from1.includes(Version.valueOf("0.1.0.SNAPSHOT")));
    }
    
    @Test
    public void testParseSingleQuotedVal() throws Exception {
        String input = "\"foo:[0,1.0.0)\"";
        assertParseListVersionRangeNames(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseSingleQuotedValWithNestedQuotes() throws Exception {
        String input = "\"foo:[0,\"1.0.0\")\"";
        assertParseListVersionRangeNames(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseMultipleVals() throws Exception {
        String input = "\"foo:[0,1.0.0)\",\"bar:[0,1.0.0)\"";
        assertParseListVersionRangeNames(input, ImmutableList.of(fooFrom0lessThan1, barFrom0lessThan1));
    }

    @Test
    public void testParseValWithExactVersion() throws Exception {
        String input = "\"foo:0.1.0\"";
        assertParseListVersionRangeNames(input, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
    }
    
    @Test
    public void testParseKeyEqualsValue() throws Exception {
        String input = "\"foo:[0,1)=foo:1\"";
        Multimap<VersionedName, VersionRangedName> expected = LinkedHashMultimap.create();
        expected.put(VersionedName.fromString("foo:1"), VersionRangedName.fromString("foo:[0,1)", false));
        assertParseListVersionRangedNameToVersionedNames(input, expected); 
    }
    
    @Test
    public void testParseKeyEqualsValueList() throws Exception {
        String input = "\"foo:[0,1)=foo:1\", foo:1-SNAPSHOT=foo:1, bar:0=bar:1";
        Multimap<VersionedName, VersionRangedName> expected = LinkedHashMultimap.create();
        expected.put(VersionedName.fromString("foo:1"), VersionRangedName.fromString("foo:[0,1)", false));
        expected.put(VersionedName.fromString("foo:1"), VersionRangedName.fromString("foo:1-SNAPSHOT", false));
        expected.put(VersionedName.fromString("bar:1"), VersionRangedName.fromString("bar:0", false));
        assertParseListVersionRangedNameToVersionedNames(input, expected); 
    }
    
    @Test
    public void testParseKeyEqualsValueWildcardsAndDefault() throws Exception {
        String input = "foo, foo:9-bogus, *, *:8, \"*:[9-bogus,9-bogut)=foo9:10.bogus\"";
        Multimap<VersionedName, VersionRangedName> expected = LinkedHashMultimap.create();
        expected.putAll(new VersionedName("foo", DEFAULT_TARGET_VERSION),
            MutableList.of(
                VersionRangedName.fromString("foo:"+DEFAULT_WILDCARD_VERSION_RANGE, false),
                VersionRangedName.fromString("foo:"+"9-bogus", false)
            ));
        expected.putAll(new VersionedName(DEFAULT_WILDCARD_NAME, DEFAULT_TARGET_VERSION),
            MutableList.of(
                VersionRangedName.fromString(DEFAULT_WILDCARD_NAME+":"+DEFAULT_WILDCARD_VERSION_RANGE, false),
                VersionRangedName.fromString(DEFAULT_WILDCARD_NAME+":8", false)
            ));
        expected.put(new VersionedName("foo9", "10.bogus"),
            VersionRangedName.fromString(DEFAULT_WILDCARD_NAME+":[9-bogus,9-bogut)", false));
        assertParseListVersionRangedNameToVersionedNames(input, expected); 
    }
    
    @Test
    public void testParseForceRemoveBundlesHeader() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("foo.bar", "1.2.3"));
        
        assertParseForceRemoveBundlesHeader("\"foo:0.1.0\"", bundle, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
        assertParseForceRemoveBundlesHeader("\"*\"", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("*", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("other:1, '*:[0,1)'", bundle, ImmutableList.of(
            new VersionRangedName("other", VersionRange.valueOf("[1.0.0,1.0.0]")), 
            new VersionRangedName("foo.bar", new VersionRange('[', Version.valueOf("0"), Version.valueOf("1"), ')'))));
    }
    
    @Test
    public void testParseForceRemoveBundlesHeaderWithSnapshot() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("foo.bar", "1.2.3.SNAPSHOT"));
        
        assertParseForceRemoveBundlesHeader("\"*\"", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
        assertParseForceRemoveBundlesHeader("*", bundle, ImmutableList.of(new VersionRangedName("foo.bar", from0lessThan1_2_3)));
    }
    
    @Test
    public void testParseForceRemoveLegacyItemsHeader() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("mybundle", "1.0.0"));
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of(
                newMockRegisteredType("foo", "1.0.0"),
                newMockRegisteredType("bar", "1.0.0")));
        
        assertParseForceRemoveLegacyItemsHeader("\"foo:0.1.0\"", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
        assertParseForceRemoveLegacyItemsHeader("\"*\"", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", from0lessThan1), new VersionRangedName("bar", from0lessThan1)));
        assertParseForceRemoveLegacyItemsHeader("*", bundle, typeSupplier, ImmutableList.of(new VersionRangedName("foo", from0lessThan1), new VersionRangedName("bar", from0lessThan1)));
        assertParseForceRemoveLegacyItemsHeader("*:1.0.0.SNAPSHOT, \"foo:[0.1,1)", bundle, typeSupplier, 
            ImmutableList.of(new VersionRangedName("foo", VersionRange.valueOf("[1.0.0.SNAPSHOT,1.0.0.SNAPSHOT]")), new VersionRangedName("bar", VersionRange.valueOf("[1.0.0.SNAPSHOT,1.0.0.SNAPSHOT]")), 
                new VersionRangedName("foo", VersionRange.valueOf("[0.1,1)"))));
    }
    
    @Test
    public void testParseBundleEmptyManifest() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of());
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        assertTrue(upgrades.isEmpty());
        assertFalse(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "0.1.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0")));
    }

    @Test
    public void testParseBundleManifestRemovals() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"foo:[0,1.0.0)\",\"foo:1.0.0.SNAPSHOT\",\"bar:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)\""));
        checkParseRemovals(bundle);
    }

    protected void checkParseRemovals(Bundle bundle) {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        assertFalse(upgrades.isEmpty());
        assertTrue(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "0.1.0")));
        assertFalse(upgrades.isBundleRemoved(new VersionedName("org.example.brooklyn.mybundle", "1.0.0")));
        
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.1.0-SNAPSHOT")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "0.0.0-SNAPSHOT")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0")));
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0.SNAPSHOT")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("foo", "1.0.0.GA")));
        
        assertTrue(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "0.1.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "1.0.0")));
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("bar", "1.0.0.SNAPSHOT")));
        
        assertFalse(upgrades.isLegacyItemRemoved(newMockCatalogItem("different", "0.1.0")));
    }
    
    @Test
    public void testParseBundleManifestUpgrades1() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)=org.example.brooklyn.mybundle:1\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "\"foo:[0,1)=foo:1\""));
        checkParseUpgrades1(bundle);
    }

    protected void checkParseUpgrades1(Bundle bundle) {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        assertFalse(upgrades.isEmpty());
        assertBundleUpgrade(upgrades, "org.example.brooklyn.mybundle", "0", "org.example.brooklyn.mybundle", "1");
        assertBundleUpgrade(upgrades, "org.example.brooklyn.mybundle", "1", null, null);
        assertTypeUpgrade(upgrades, "foo", "0", "foo", "1");
        assertTypeUpgrade(upgrades, "foo", "0.2", "foo", "1");
        assertTypeUpgrade(upgrades, "foo", "0-SNAPSHOT", "foo", "1");
        assertTypeUpgrade(upgrades, "foo", "1", null, null);
    }
    
    @Test
    public void testParseBundleManifest() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"foo:[0,1.0.0)\",\"foo:1.0.0.SNAPSHOT\",\"bar:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)=org.example.brooklyn.mybundle:1\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "\"foo:[0,1)=foo:1\""));
        checkParseRemovals(bundle);
        checkParseUpgrades1(bundle);
    }

    @Test
    public void testParseBundleManifestWithSpaces() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"foo:[0,1.0.0)\", \"foo:1.0.0.SNAPSHOT\", \"bar:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, " \"org.example.brooklyn.mybundle:[0,1.0.0)\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"org.example.brooklyn.mybundle:[0,1.0.0)=org.example.brooklyn.mybundle:1\" ",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "\"foo:[0,1)=foo:1\""));
        checkParseRemovals(bundle);
        checkParseUpgrades1(bundle);
    }


    @Test
    public void testParseBundleManifestUpgrades2() throws Exception {
        Bundle bundle = newMockBundle(new VersionedName("bun", "2"), 
            ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, 
                    "foo, foo:9-bogus, *, *:8, \"*:[9-bogus,9-bogut)=foo9:10.bogus\"",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, 
                    "foo=foo:3, foo:9-bogus=foo:3, *, *:8, bar:2-SNAPSHOT, \"*:[9-bogus,9-bogut)=foo9:10.bogus\""));
        checkParseUpgrades2(bundle);
    }

    protected void checkParseUpgrades2(Bundle bundle) {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of(
            new BasicRegisteredType(null, "bar", "2", null),
            new BasicRegisteredType(null, "bub", "2", null)));
        CatalogUpgrades upgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
        
        assertFalse(upgrades.isEmpty());
        
        assertBundleUpgrade(upgrades, "foo", "0.1", "bun", "2.0.0");
        assertBundleUpgrade(upgrades, "foo", "9-bogus", "bun", "2.0.0");
        assertBundleUpgrade(upgrades, "foo", "1.5", "bun", "2.0.0");
        assertBundleUpgrade(upgrades, "foo", "3", null, null);
        
        assertBundleUpgrade(upgrades, "bun", "1", "bun", "2.0.0");
        assertBundleUpgrade(upgrades, "bun", "3", null, null);
        
        assertTypeUpgrade(upgrades, "foo", "1", "foo", "3");
        assertTypeUpgrade(upgrades, "foo", "2.2", null, null);
        assertTypeUpgrade(upgrades, "foo", "9-bogus", "foo", "3");
        
        assertTypeUpgrade(upgrades, "bar", "1", "bar", "2.0.0");
        assertTypeUpgrade(upgrades, "bar", "8", "bar", "2.0.0");
        assertTypeUpgrade(upgrades, "bar", "2-SNAPSHOT", "bar", "2.0.0");
        assertTypeUpgrade(upgrades, "bar", "2.0.0.SNAPSHOT", "bar", "2.0.0");
        
        assertTypeUpgrade(upgrades, "bub", "1", "bub", "2.0.0");
        assertTypeUpgrade(upgrades, "bub", "8", "bub", "2.0.0");
        assertTypeUpgrade(upgrades, "bub", "2-SNAPSHOT", null, null);
        
        assertTypeUpgrade(upgrades, "bar", "9-bogus-one", "foo9", "10.bogus");
        assertTypeUpgrade(upgrades, "bar", "9.bogus", "foo9", "10.bogus");
        assertTypeUpgrade(upgrades, "baz", "9-bogus-one", null, null);
        assertTypeUpgrade(upgrades, "bar", "9-bogut", null, null);
        assertTypeUpgrade(upgrades, "bar", "9.bogut", null, null);
    }
    
    @Test
    public void testParseBundleManifestUpgradesValidatesIfNoBundleUpgradeVersion() throws Exception {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of(
            new BasicRegisteredType(null, "foo", "1.0", null) ));
        Bundle bundle = newMockBundle(new VersionedName("bundle", "1.0"), 
            ImmutableMap.of(BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "foo"));
        try {
            BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "foo", "version", "cannot be inferred");
        }
    }
    
    @Test
    public void testParseBundleManifestUpgradesValidatesIfTypeNotContained() throws Exception {
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        Bundle bundle = newMockBundle(new VersionedName("bundle", "1.0"), 
            ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "*",
                BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_TYPES, "foo"));
        try {
            BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "foo", "bundle", "upgrade", "does not contain", "target");
        }
    }
    
    private void assertBundleUpgrade(CatalogUpgrades upgrades, String sn, String sv, String tn, String tv) {
        Set<VersionedName> targets = upgrades.getUpgradesForBundle(new VersionedName(sn, sv));
        if (tn==null) {
            Asserts.assertSize(targets, 0);
        } else if (!targets.contains(new VersionedName(tn, tv))) {
            Assert.fail("Failed target "+tn+":"+tv+" expected for "+sn+":"+sv+"; got "+targets);
        }
    }

    private void assertTypeUpgrade(CatalogUpgrades upgrades, String sn, String sv, String tn, String tv) {
        Set<VersionedName> targets = upgrades.getUpgradesForType(new VersionedName(sn, sv));
        if (tn==null) {
            Asserts.assertSize(targets, 0);
        } else if (!targets.contains(new VersionedName(tn, tv))) {
            Assert.fail("Failed target "+tn+":"+tv+" expected for "+sn+":"+sv+"; got "+targets);
        }
    }

    @Test
    public void testForgetQuotesGivesNiceError() throws Exception {
        Bundle bundle = newMockBundle(ImmutableMap.of(
                BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "foo:[0,1.0.0),bar:[0,1.0.0)"));
        Supplier<Iterable<RegisteredType>> typeSupplier = Suppliers.ofInstance(ImmutableList.of());
        
        try {
            BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "quote");
        }
    }

    @Test
    public void testStripQuotes() throws Exception {
        assertEquals(BundleUpgradeParser.stripQuotes("a"), "a");
        assertEquals(BundleUpgradeParser.stripQuotes("'a'"), "a");
        assertEquals(BundleUpgradeParser.stripQuotes("\"\""), "");
        assertEquals(BundleUpgradeParser.stripQuotes("''"), "");
    }
    
    private Bundle newMockBundle(Map<String, String> rawHeaders) {
        return newMockBundle(VersionedName.fromString("do.no.care:1.2.3"), rawHeaders);
    }

    private Bundle newMockBundle(VersionedName name) {
        return newMockBundle(name, ImmutableMap.of());
    }
    
    private Bundle newMockBundle(VersionedName name, Map<String, String> rawHeaders) {
        Dictionary<String, String> headers = new Hashtable<>(rawHeaders);
        Bundle result;
        try {
            result = Mockito.mock(Bundle.class);
        } catch (Exception e) {
            throw new IllegalStateException("Java too old.  There is a bug in really early java 1.8.0 "
                + "that causes mocks to fail, and has probably caused this.", e);
        }
        Mockito.when(result.getHeaders()).thenReturn(headers);
        Mockito.when(result.getSymbolicName()).thenReturn(name.getSymbolicName());
        Mockito.when(result.getVersion()).thenReturn(Version.valueOf(name.getOsgiVersionString()));
        return result;
    }

    private RegisteredType newMockRegisteredType(String symbolicName, String version) {
        RegisteredType result = Mockito.mock(RegisteredType.class);
        Mockito.when(result.getSymbolicName()).thenReturn(symbolicName);
        Mockito.when(result.getVersion()).thenReturn(version);
        Mockito.when(result.getVersionedName()).thenReturn(new VersionedName(symbolicName, version));
        return result;
    }

    private CatalogItem<?,?> newMockCatalogItem(String symbolicName, String version) {
        CatalogItem<?,?> result = Mockito.mock(CatalogItem.class);
        Mockito.when(result.getSymbolicName()).thenReturn(symbolicName);
        Mockito.when(result.getVersion()).thenReturn(version);
        Mockito.when(result.getId()).thenReturn(symbolicName+":"+version);
        Mockito.when(result.getCatalogItemId()).thenReturn(symbolicName+":"+version);
        return result;
    }
    
    private void assertParseListVersionRangeNames(String input, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseVersionRangedNameList(input, false, MutableList.of(DEFAULT_WILDCARD_NAME), DEFAULT_WILDCARD_VERSION);
        assertListsEqual(actual, expected);
    }
    
    private void assertParseListVersionRangedNameToVersionedNames(String input, Multimap<VersionedName,VersionRangedName> expected) throws Exception {
        Multimap<VersionedName, VersionRangedName> actual = BundleUpgradeParser.parseVersionRangedNameEqualsVersionedNameList(input, false, 
            MutableList.of(DEFAULT_WILDCARD_NAME), MutableList.of(DEFAULT_WILDCARD_VERSION_RANGE),
            (i) -> new VersionedName(i.getSymbolicName(), DEFAULT_TARGET_VERSION));
        assertEquals(actual, expected);
    }
    
    private void assertParseForceRemoveBundlesHeader(String input, Bundle bundle, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseForceRemoveBundlesHeader(input, bundle);
        assertListsEqual(actual, expected);
    }

    private void assertParseForceRemoveLegacyItemsHeader(String input, Bundle bundle, Supplier<? extends Iterable<? extends RegisteredType>> typeSupplier, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseForceRemoveLegacyItemsHeader(input, bundle, typeSupplier);
        assertListsEqual(actual, expected);
    }

    private void assertListsEqual(List<VersionRangedName> actual, List<VersionRangedName> expected) throws Exception {
        String errMsg = "actual="+actual;
        assertEquals(actual.size(), expected.size(), errMsg); 
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(actual.get(i).getSymbolicName(), expected.get(i).getSymbolicName(), errMsg);
            assertEquals(actual.get(i).getOsgiVersionRange(), expected.get(i).getOsgiVersionRange(), errMsg);
        }
    }

    private void assertVersionRangedNameFails(String input, String expectedFailure, String... optionalOtherExpectedFailures) {
        try {
            VersionRangedName.fromString(input, false);
            Asserts.shouldHaveFailedPreviously();
        } catch (IllegalArgumentException e) {
            Asserts.expectedFailureContains(e, expectedFailure, optionalOtherExpectedFailures);
        }
    }
}
