package org.apache.brooklyn.util.yorml;

public class YormlContext {

    final String jsonPath;
    final String expectedType;
    Object javaObject;
    Object yamlObject;
    
    public YormlContext(String jsonPath, String expectedType) {
        this.jsonPath = jsonPath;
        this.expectedType = expectedType;
    }
    
    public Object getJavaObject() {
        return javaObject;
    }
    public void setJavaObject(Object javaObject) {
        this.javaObject = javaObject;
    }
    public Object getYamlObject() {
        return yamlObject;
    }
    public void setYamlObject(Object yamlObject) {
        this.yamlObject = yamlObject;
    }
    
}
