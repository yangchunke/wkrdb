package net.yck.wrkdb.meta;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.meta.Attribute;
import net.yck.wrkdb.meta.DataElement;
import net.yck.wrkdb.meta.NamedElement;
import net.yck.wrkdb.meta.Schema;


public class AttributeTestSuite extends MetaTestSuiteBase {

    @Test
    public void test() {
        for (int i = 0; i < c_Iteration; i++) {
            Attribute expected = newAttribute(null, rand, nextDataType(rand, false));
            String json = expected.toString();
            Attribute actual = NamedElement.fromJson(json, Attribute.class);
            Assert.assertEquals(expected, actual);
        }
    }

    static Attribute newAttribute(Schema schema, Random rand, DataElement.Type dataType) {
        return (Attribute) new Attribute()//
                        .setType(dataType)//
                        .setUdt(dataType == DataElement.Type.UDT ? nextUDT(schema, rand) : StringUtils.EMPTY)//
                        .setNullable(rand == null ? false : rand.nextBoolean())//
                        .setName(UUID.randomUUID().toString());
    }

    static Attribute newAttribute(String name, DataElement.Type dataType, String udt, boolean nullable) {
        return (Attribute) new Attribute()//
                        .setType(dataType)//
                        .setUdt(udt)//
                        .setNullable(nullable)//
                        .setName(name);
    }
}
