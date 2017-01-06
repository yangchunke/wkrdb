package net.yck.wrkdb.meta;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.meta.Attribute;
import net.yck.wrkdb.meta.DataElement;
import net.yck.wrkdb.meta.NamedElement;
import net.yck.wrkdb.meta.Schema;
import net.yck.wrkdb.meta.UDT;

public class UDTTestSuite extends MetaTestSuiteBase {

    public final static String c_SampleUDTName = "Pair";

    @Test
    public void test() {
        for (int i = 0; i < c_Iteration; i++) {
            UDT expected = newUDT(null, rand);
            String json = expected.toString();
            UDT actual = NamedElement.fromJson(json, UDT.class);
            Assert.assertEquals(expected, actual);
        }
    }

    static UDT newUDT(Schema schema, Random rand) {
        UDT ret = (UDT) new UDT().setName(UUID.randomUUID().toString());
        final int numOfAttrs = rand == null ? 1 : rand.nextInt(c_MaxNumOfAttributes);
        for (int i = 0; i < numOfAttrs; i++) {
            ret.addAttribute(AttributeTestSuite.newAttribute(schema, rand, nextDataType(rand, schema != null && schema.getUDTs().isEmpty())));
        }
        return ret;
    }

    private static UDT newUDT(String name, Collection<Attribute> attributes) {
        UDT ret = (UDT) new UDT().setName(name);
        ret.getAttributes().addAll(attributes);
        return ret;
    }

    static UDT sampleUDT() {
        return UDTTestSuite.newUDT(c_SampleUDTName, //
                        Arrays.asList(AttributeTestSuite.newAttribute("left", DataElement.Type.STRING, StringUtils.EMPTY, false), //
                                        AttributeTestSuite.newAttribute("right", DataElement.Type.TIMESTAMP, StringUtils.EMPTY, false)));
    }
}
