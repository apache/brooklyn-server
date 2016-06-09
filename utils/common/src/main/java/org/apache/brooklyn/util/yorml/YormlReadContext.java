package org.apache.brooklyn.util.yorml;

public class YormlReadContext extends YormlContext {

    public YormlReadContext(String jsonPath, String expectedType) {
        super(jsonPath, expectedType);
    }
    
    String origin;
    int offset;
    int length;
    
}
