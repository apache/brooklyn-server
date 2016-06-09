package org.apache.brooklyn.util.yorml;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;

public class YormlConverter {

    private final YormlConfig config;
    
    public YormlConverter(YormlConfig config) {
        this.config = config;
    }

    /**
     * returns object of type expectedType
     * makes shallow copy of the object, then goes through serializers modifying it or creating/setting result,
     * until result is done
     */ 
    public Object read(YormlReadContext context) {
        List<YormlSerializer> serializers = MutableList.<YormlSerializer>of().appendAll(config.serializersPost);
        int i=0;
        Map<Object,Object> blackboard = MutableMap.of();
        while (i<serializers.size()) {
            YormlSerializer s = serializers.get(i);
            YormlContinuation next = s.read(context, config, blackboard);
            if (next == YormlContinuation.FINISHED) break;
            else if (next == YormlContinuation.RESTART) i=0;
            else i++;
        }
        // TODO check failures?
        return context.getJavaObject();
    }

    /**
     * returns jsonable object (map, list, primitive) 
     */   
    public Object write(YormlContext context) {
        List<YormlSerializer> serializers = MutableList.<YormlSerializer>of().appendAll(config.serializersPost);
        int i=0;
        Map<Object,Object> blackboard = MutableMap.of();
        while (i<serializers.size()) {
            YormlSerializer s = serializers.get(i);
            YormlContinuation next = s.write(context, config, blackboard);
            if (next == YormlContinuation.FINISHED) break;
            else if (next == YormlContinuation.RESTART) i=0;
            else i++;
        }
        // TODO check failures?
        return context.getYamlObject();
    }

    /**
     * generates human-readable schema for a type
     */
    public String document(String type) {
        // TODO
        return null;
    }

}
