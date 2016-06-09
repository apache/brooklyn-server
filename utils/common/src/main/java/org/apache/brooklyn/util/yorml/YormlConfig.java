package org.apache.brooklyn.util.yorml;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;

public class YormlConfig {

    YormlTypeRegistry typeRegistry;
    List<YormlSerializer> serializersPost = MutableList.of();
    
    public YormlTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }
    
}
