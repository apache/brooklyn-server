package org.apache.brooklyn.util.core.xstream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.thoughtworks.xstream.XStream;
import junit.framework.TestCase;
import org.assertj.core.util.Strings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;

public class HashMultimapConverterTest extends ConverterTestFixture {

    @Override
    protected void registerConverters(XStream xstream) {
        super.registerConverters(xstream);
        xstream.registerConverter(new HashMultimapConverter(xstream.getMapper()));
    }

    @Test
    public void testHashMultimapEmpty() throws UnknownHostException {
        String fmr = Strings.concat(
                "<com.google.common.collect.HashMultimap serialization=\"custom\">\n",
                "  <unserializable-parents/>\n",
                "  <com.google.common.collect.HashMultimap>\n",
                "    <default/>\n",
                "    <int>0</int>\n",
                "  </com.google.common.collect.HashMultimap>\n",
                "</com.google.common.collect.HashMultimap>");
        assertX(HashMultimap.create(), fmr);
    }

    @Test
    public void testHashMultimapBasic() throws UnknownHostException {
        String fmr = Strings.concat(
                "<com.google.common.collect.HashMultimap serialization=\"custom\">\n",
                "  <unserializable-parents/>\n",
                "  <com.google.common.collect.HashMultimap>\n",
                "    <default/>\n",
                "    <int>0</int>\n",
                "  </com.google.common.collect.HashMultimap>\n",
                "</com.google.common.collect.HashMultimap>");

        HashMultimap<Object, Object> hashMultimap = HashMultimap.create();
        hashMultimap.put("one", "one");
        hashMultimap.put("two", "two");
        hashMultimap.put("two", "two.two");
        assertX(hashMultimap, fmr);
    }

}