package net.yck.wrkdb.server.meta;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.server.meta.Column;
import net.yck.wrkdb.server.meta.DataElement;
import net.yck.wrkdb.server.meta.NamedElement;
import net.yck.wrkdb.server.meta.Schema;

public class ColumnTestSuite extends MetaTestSuiteBase {

    private static Random rand = new Random(System.currentTimeMillis());

    @Test
    public void test() {
        for (int i = 0; i < c_Iteration; i++) {
            Column expected = newColumn(null, rand, nextDataType(rand, false));
            String json = expected.toString();
            Column actual = NamedElement.fromJson(json, Column.class);
            Assert.assertEquals(expected, actual);
        }
    }

    static Column newColumn(Schema schema, Random rand, DataElement.Type dataType) {
        return (Column) new Column()//
                        .setType(dataType)//
                        .setNullable(rand == null ? false : rand.nextBoolean())//
                        .setUdt(dataType == DataElement.Type.UDT ? nextUDT(schema, rand) : StringUtils.EMPTY)//
                        .setName(UUID.randomUUID().toString());
    }

    static Column newColumn(String name, DataElement.Type dataType, String udt, boolean nullable) {
        return (Column) new Column()//
                        .setType(dataType)//
                        .setUdt(udt)//
                        .setNullable(nullable)//
                        .setName(name);
    }

}
