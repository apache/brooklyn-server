package org.apache.brooklyn.util.core.xstream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Serialization;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.SingleValueConverter;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;
import org.apache.brooklyn.util.javalang.ReflectionsTest.K;

/** serialization of HashMultimap changed after Guava 18;
 * to support pre-Guava-18 serialized objects we just continue to use the old logic.
 * To test, note that this is testable with the RebingTest.testSshFeed_2018_...
 */
public class HashMultimapConverter implements Converter {
    private final Mapper mapper;

    public HashMultimapConverter(Mapper mapper) {
        this.mapper = mapper;
    }

    // TODO how do we convert this from ObjectOutputStream to XStream mapper ?
    // (code below copied from HashMultimap.readObject / writeObject)

//    @Override
//    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
//        stream.defaultWriteObject();
//        stream.writeInt(expectedValuesPerKey);
//        Serialization.writeMultimap(this, stream);
//    }
//
//    @Override
//    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
//        stream.defaultReadObject();
//        expectedValuesPerKey = stream.readInt();
//        int distinctKeys = Serialization.readCount(stream);
//        Map<K, Collection<V>> map = Maps.newHashMapWithExpectedSize(distinctKeys);
//        setMap(map);
//        Serialization.populateMultimap(this, stream, distinctKeys);
//    }

    @Override
    public boolean canConvert(Class type) {
        return type.isAssignableFrom(HashMultimap.class);
    }
}
