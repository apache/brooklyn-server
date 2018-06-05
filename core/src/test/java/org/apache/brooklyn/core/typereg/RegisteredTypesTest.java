package org.apache.brooklyn.core.typereg;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import static org.testng.Assert.*;

public class RegisteredTypesTest {

    public static final String DRBD_YAML = "brooklyn.catalog:\n" +
            "  version: \"1.0.0-SNAPSHOT\" # BROOKLYN_DRBD_VERSION\n" +
            "  publish:\n" +
            "    description: |\n" +
            "      DRBD is a distributed replicated storage system for the Linux platform. It is implemented as a kernel driver, \n" +
            "      several userspace management applications, and some shell scripts. DRBD is traditionally used in high availability (HA) computer clusters to provide a\n" +
            "      RAID 1 like configuration over a network.\n" +
            "    license_code: Apache-2.0\n" +
            "    defaults:\n" +
            "      drbdIconUrl: \"https://s3.eu-central-1.amazonaws.com/misc-csft/drbd.jpg\"\n" +
            "      classpathIconUrl: &drbdIconUrl 'classpath://io.brooklyn.drbd.brooklyn-drbd:drbd.png'\n" +
            "  items:\n" +
            "  - \"https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.bom\"\n" +
            "  - id: drbd-node\n";

    public static final String DRBD_TESTS_YAML = "brooklyn.catalog:\n" +
            "  version: \"1.0.0-SNAPSHOT\" # BROOKLYN_DRBD_VERSION\n" +
            "  items:\n" +
            "  - https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.tests.bom\n" +
            "  - id: drbd-test\n" +
            "    name: DRBD Test\n" +
            "    itemType: template\n" +
            "    iconUrl: https://s3.eu-central-1.amazonaws.com/misc-csft/drbd.jpg\n" +
            "    license: Apache-2.0\n" +
            "    item:\n" +
            "      booklyn.config:\n" +
            "        defaultDisplayName: DRBD Test\n" +
            "      services:\n" +
            "      - type: drbd-webapp\n";

    public static final String DRBR_YAML_WITH_COMMENTS = DRBD_YAML + "\n" +
            "# this is a comment";
    private static final String DRBR_YAML_WITHOUT_QUOTES = "brooklyn.catalog:\n" +
            "  version: 1.0.0-SNAPSHOT # BROOKLYN_DRBD_VERSION\n" +
            "  publish:\n" +
            "    description: |\n" +
            "      DRBD is a distributed replicated storage system for the Linux platform. It is implemented as a kernel driver, \n" +
            "      several userspace management applications, and some shell scripts. DRBD is traditionally used in high availability (HA) computer clusters to provide a\n" +
            "      RAID 1 like configuration over a network.\n" +
            "    license_code: Apache-2.0\n" +
            "    defaults:\n" +
            "      drbdIconUrl: https://s3.eu-central-1.amazonaws.com/misc-csft/drbd.jpg\n" +
            "      classpathIconUrl: &drbdIconUrl 'classpath://io.brooklyn.drbd.brooklyn-drbd:drbd.png'\n" +
            "  items:\n" +
            "  - https://github.com/brooklyncentral/common-catalog-utils/releases/download/v0.1.0/common.bom\n" +
            "  - id: drbd-node\n";

    @Test
    public void testIdenticalPlansAreEquivalent() {
        BasicRegisteredType a = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType b = makeTypeWithYaml(DRBD_YAML);

        boolean plansEquivalent = RegisteredTypes.arePlansEquivalent(
                a, b);
        assertTrue(plansEquivalent);
    }

    @Test
    public void testDifferentPlansAreNotEquivalent() {
        BasicRegisteredType a = makeTypeWithYaml(DRBD_YAML);
        BasicRegisteredType b = makeTypeWithYaml(DRBD_TESTS_YAML);

        boolean plansEquivalent = RegisteredTypes.arePlansEquivalent(
                a, b);
        assertFalse(plansEquivalent);
    }

    @Test
    public void testComparisonIgnoresComments() {
        BasicRegisteredType a = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType b = makeTypeWithYaml(DRBR_YAML_WITH_COMMENTS);

        boolean plansEquivalent = RegisteredTypes.arePlansEquivalent(
                a, b);
        assertTrue(plansEquivalent);
    }

    @Test
    public void testComparisonIgnoresQuotedStrings() {
        BasicRegisteredType a = makeTypeWithYaml(DRBD_YAML);

        BasicRegisteredType b = makeTypeWithYaml(DRBR_YAML_WITHOUT_QUOTES);

        boolean plansEquivalent = RegisteredTypes.arePlansEquivalent(
                a, b);
        assertTrue(plansEquivalent);
    }

    BasicRegisteredType makeTypeWithYaml(final String yaml) {
        return new BasicRegisteredType(
                BrooklynTypeRegistry.RegisteredTypeKind.SPEC,
                "B",
                "1",
                new RegisteredType.TypeImplementationPlan() {
                    @Nullable
                    @Override
                    public String getPlanFormat() {
                        return null;
                    }

                    @Override
                    public Object getPlanData() {
                        return yaml;
                    }
                });
    }


}