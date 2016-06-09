package org.apache.brooklyn.util.yorml.tests;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yorml.YormlTypeRegistry;

public class MockYormlTypeRegistry implements YormlTypeRegistry {

    Map<String,Class<?>> types = MutableMap.of();
    
    @Override
    public Object newInstance(String typeName) {
        Class<?> type = types.get(typeName);
        if (type==null) return null;
        try {
            return type.newInstance();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    public void put(String typeName, Class<?> type) {
        types.put(typeName, type);
    }

}
