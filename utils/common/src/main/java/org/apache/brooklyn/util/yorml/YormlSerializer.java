package org.apache.brooklyn.util.yorml;

import java.util.Map;

import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;

public interface YormlSerializer {

    /**
     * modifies yaml object and/or java object and/or blackboard as appropriate,
     * when trying to build a java object from a yaml object,
     * returning true if it did anything (and so should restart the cycle).
     * implementations must NOT return true indefinitely if passed the same instances!
     */ 
    public YormlContinuation read(YormlReadContext context, YormlConfig config, Map<Object,Object> blackboard);

    /**
     * modifies java object and/or yaml object and/or blackboard as appropriate,
     * when trying to build a yaml object from a java object,
     * returning true if it did anything (and so should restart the cycle).
     * implementations must NOT return true indefinitely if passed the same instances!
     */   
    public YormlContinuation write(YormlContext context, YormlConfig config, Map<Object,Object> blackboard);

    /**
     * generates human-readable schema for a type using this schema.
     */
    public String document(String type, YormlConfig config);

}
