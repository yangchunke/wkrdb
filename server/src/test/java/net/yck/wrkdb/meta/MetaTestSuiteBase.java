package net.yck.wrkdb.meta;

import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import net.yck.wrkdb.ITestSuite;
import net.yck.wrkdb.meta.DataElement;
import net.yck.wrkdb.meta.Schema;
import net.yck.wrkdb.meta.UDT;

abstract class MetaTestSuiteBase implements ITestSuite {

    final protected static int c_MaxNumOfAttributes = 64;
    final protected static int c_MaxNumOfColumns = 64;
    final protected static int c_MaxNumOfClusteringKeys = 8;
    final protected static int c_MaxNumOfKeyColumns = 5;
    final protected static int c_MaxNumOfTables = 4;
    final protected static int c_MaxNumOfUDTs = 16;
    final protected static int c_MaxNumOfSchemas = 32;

    protected static DataElement.Type nextDataType(Random rand, boolean noUDT) {
        return rand == null || noUDT ? DataElement.Type.values()[0] : DataElement.Type.values()[rand.nextInt(DataElement.Type.values().length)];
    }

    protected static String nextUDT(Schema schema, Random rand) {
        if (schema == null)
            return UUID.randomUUID().toString();
        Iterator<UDT> iter = schema.getUDTs().iterator();
        int marker = rand.nextInt(schema.getUDTs().size());
        for (int i = 0; i < marker && iter.hasNext(); i++, iter.next())
            ;
        return iter.next().getName();
    }
}
