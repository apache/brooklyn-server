package org.apache.brooklyn.util.yorml.tests;

import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.yorml.Yorml;
import org.testng.Assert;
import org.testng.annotations.Test;

public class YormlBasicTests {

    public static class Shape {
        String name;
        String color;
        int size;
    }
    
    @Test
    public void testReadJavaType() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        tr.put("shape", Shape.class);
        Yorml y = Yorml.newInstance(tr);
        Object resultO = y.read("{ type: shape }", "object");
        
        Assert.assertNotNull(resultO);
        Assert.assertTrue(resultO instanceof Shape, "Wrong result type: "+resultO);
        Shape result = (Shape)resultO;
        Assert.assertNull(result.name);
        Assert.assertNull(result.color);
    }
    
    @Test
    public void testReadFieldInFields() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        tr.put("shape", Shape.class);
        Yorml y = Yorml.newInstance(tr);
        Object resultO = y.read("{ type: shape, fields: { color: red } }", "object");
        
        Assert.assertNotNull(resultO);
        Assert.assertTrue(resultO instanceof Shape, "Wrong result type: "+resultO);
        Shape result = (Shape)resultO;
        Assert.assertNull(result.name);
        Assert.assertEquals(result.color, "red");
    }

    @Test
    public void testWriteFieldInFields() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        tr.put("shape", Shape.class);
        Yorml y = Yorml.newInstance(tr);
        
        Shape s = new Shape();
        s.color = "red";
        Object resultO = y.write(s);
        
        Assert.assertNotNull(resultO);
        String out = Jsonya.newInstance().add(resultO).toString();
        String expected = Jsonya.newInstance().add("type", "java"+":"+Shape.class.getPackage().getName()+"."+YormlBasicTests.class.getSimpleName()+"$"+"Shape")
                .at("fields").add("color", "red", "size", 0).root().toString();
        Assert.assertEquals(out, expected);
    }

}
