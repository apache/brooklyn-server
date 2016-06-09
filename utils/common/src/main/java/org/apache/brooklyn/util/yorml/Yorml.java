package org.apache.brooklyn.util.yorml;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yorml.serializers.FieldsInFieldsMap;
import org.apache.brooklyn.util.yorml.serializers.InstantiateType;


public class Yorml {

    YormlConfig config;
    
    private Yorml() {}
    
    public static Yorml newInstance(YormlTypeRegistry typeRegistry) {
        return newInstance(typeRegistry, MutableList.of(
            new FieldsInFieldsMap(),
            new InstantiateType() ));
    }
    
    public static Yorml newInstance(YormlTypeRegistry typeRegistry, List<YormlSerializer> serializers) {
        Yorml result = new Yorml();
        result.config = new YormlConfig();
        result.config.typeRegistry = typeRegistry;
        result.config.serializersPost.addAll(serializers);
        
        return result;
    }
    
    public Object read(String yaml) {
        return read(yaml, null);
    }
    public Object read(String yaml, String type) {
        Object yamlObject = new org.yaml.snakeyaml.Yaml().load(yaml);
        YormlReadContext context = new YormlReadContext("", type);
        context.setYamlObject(yamlObject);
        new YormlConverter(config).read(context);
        return context.getJavaObject();
    }

    public Object write(Object java) {
        YormlContext context = new YormlContext("", null);
        context.setJavaObject(java);
        new YormlConverter(config).write(context);
        return context.getYamlObject();
    }
    
//    public <T> T read(String yaml, Class<T> type) {
//    }
//    public <T> T read(String yaml, TypeToken<T> type) {
//    }
    
}
