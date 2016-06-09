package org.apache.brooklyn.util.yorml;

public interface YormlTypeRegistry {

    Object newInstance(String type);
    
}
