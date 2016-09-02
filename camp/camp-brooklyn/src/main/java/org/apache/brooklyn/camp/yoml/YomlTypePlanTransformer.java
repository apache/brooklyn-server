package org.apache.brooklyn.camp.yoml;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.typereg.AbstractFormatSpecificTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.AbstractTypePlanTransformer;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;

import com.google.common.collect.ImmutableList;

/** 
 * Makes it possible for Brooklyn to resolve YOML items,
 * both types registered with the system using YOML
 * and plans in the YOML format (sent here as unregistered types),
 * and supporting any objects and special handling for "spec" objects.
 * 
 * DONE
 * - test programmatic addition and parse (beans) with manual objects
 * 
 * NEXT
 * - attach custom serializers
 * - support specs from yoml
 * - $brooklyn:object(format): <object-in-given-format>
 *   and $brooklyn:yoml(expected-type): <yaml-object>
 * - catalog add arbitrary types via yoml, specifying format
 * - catalog impl in yoml as test?
 * - REST API for deploy accepts specific format, can call yoml (can we test this earlier?)
 * 
 * THEN
 * - generate its own documentation
 * - persist to yoml
 * - yoml allows `constructor: [list]` and `constructor: { mode: static, type: factory, method: newInstance, args: ... }`
 *   and maybe even `constructor: { mode: chain, steps: [ { mode: constructor, type: Foo.Builder }, { mode: method, method: bar, args: [ true ] }, { mode: method, method: build } ] }`  
 * - type access control and java instantiation access control ?
 */
public class YomlTypePlanTransformer extends AbstractTypePlanTransformer {

    private static final List<String> FORMATS = ImmutableList.of("yoml");
    
    public static final String FORMAT = FORMATS.get(0);
    
    public YomlTypePlanTransformer() {
        super(FORMAT, "YOML Brooklyn syntax", "Standard YOML adapters for Apache Brooklyn including OASIS CAMP");
    }

    @Override
    protected double scoreForNullFormat(Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        Maybe<Map<?,?>> plan = RegisteredTypes.getAsYamlMap(planData);
        if (plan.isAbsent()) return 0;
        int score = 0;
        if (plan.get().containsKey("type")) score += 5;
        if (plan.get().containsKey("services")) score += 2;
        // TODO these should become legacy
        if (plan.get().containsKey("brooklyn.locations")) score += 1;
        if (plan.get().containsKey("brooklyn.policies")) score += 1;
        
        if (score==0) return 0.1;
        return (1.0 - 1.0/score);
    }

    @Override
    protected double scoreForNonmatchingNonnullFormat(String planFormat, Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        if (FORMATS.contains(planFormat.toLowerCase())) return 0.9;
        return 0;
    }

    @Override
    protected AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        // TODO
        return null;
    }

    @Override
    protected Object createBean(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        YomlTypeRegistry tr = new BrooklynYomlTypeRegistry(mgmt, context);
        Yoml y = Yoml.newInstance(tr);
        // TODO could cache the parse, could cache the instantiation instructions
        Object data = type.getPlan().getPlanData();
        
        Class<?> expectedSuperType = context.getExpectedJavaSuperType();
        String expectedSuperTypeName = tr.getTypeNameOfClass(expectedSuperType);
        
        if (data instanceof String) {
            return y.read((String) data, expectedSuperTypeName);
        } else {
            // could do this
//            return y.readFromYamlObject(data, expectedSuperTypeName);
            // but it should always be a string...
            throw new IllegalArgumentException("The implementation plan for '"+type+"' should be a string in order to process as YOML");
        }
    }
    
    @Override
    public double scoreForTypeDefinition(String formatCode, Object catalogData) {
        // TODO catalog parsing
        return 0;
    }

    @Override
    public List<RegisteredType> createFromTypeDefinition(String formatCode, Object catalogData) {
        // TODO catalog parsing
        return null;
    }

    
    public static class YomlTypeImplementationPlan extends AbstractFormatSpecificTypeImplementationPlan<String> {
        public YomlTypeImplementationPlan(TypeImplementationPlan otherPlan) {
            super(FORMATS.get(0), String.class, otherPlan);
        }
        public YomlTypeImplementationPlan(String planData) {
            this(new BasicTypeImplementationPlan(FORMATS.get(0), planData));
        }
    }
}
