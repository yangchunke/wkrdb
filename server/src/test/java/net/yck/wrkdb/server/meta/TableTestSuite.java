package net.yck.wrkdb.server.meta;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.server.meta.Column;
import net.yck.wrkdb.server.meta.DataElement;
import net.yck.wrkdb.server.meta.Group;
import net.yck.wrkdb.server.meta.Key;
import net.yck.wrkdb.server.meta.NamedElement;
import net.yck.wrkdb.server.meta.Schema;
import net.yck.wrkdb.server.meta.Table;

public class TableTestSuite extends MetaTestSuiteBase {

    public final static String c_SampleTableName = "Sample";
    public final static String c_SampleGroupName_Val = "Val";
    public final static String c_SampleGroupName_Addr = "Addr";
    public final static String c_SampleClusteringKeyName = "CK";

    @Test
    public void test() {
        for (int i = 0; i < c_Iteration; i++) {
            Table expected = i == 0 ? sampleTable() : newTable(null, rand);
            String json = expected.toString();
            Table actual = NamedElement.fromJson(json, Table.class);
            Assert.assertEquals(expected, actual);
        }
    }

    static Table newTable(Schema schema, Random rand) {
        Table table = (Table) new Table()//
                        .setName(UUID.randomUUID().toString());

        final int numOfColumns = rand == null ? 1 : Math.max(1, rand.nextInt(c_MaxNumOfColumns));
        for (int i = 0; i < numOfColumns; i++) {
            Column column = ColumnTestSuite.newColumn(schema, rand, nextDataType(rand, schema != null && schema.getUDTs().isEmpty()));
            table.addColumn(column);
        }

        Column col = table.getColumns().iterator().next();
        final int numOfColumnGroups = rand == null ? 1 : Math.max(1, rand.nextInt(numOfColumns));
        for (int i = 0; i < numOfColumnGroups; i++) {
            Group group = (Group) new Group()//
                            .addColumn(col)//
                            .setName(UUID.randomUUID().toString());
            table.addGroup(group);
        }

        table.setPartitionKey((Key.Partition) new Key.Partition().addColumn(col));
        if (rand == null || rand.nextBoolean()) {
            table.setRowKey((Key.Row) new Key.Row().addColumn(col));
        }
        if (rand == null || rand.nextBoolean()) {
            table.addClusteringKey((Key.Clustering) new Key.Clustering().addColumn(col).setName(UUID.randomUUID().toString()));
        }

        return table;
    }

    static Table sampleTable() {
        Table table = (Table) new Table()//
                        .setName(c_SampleTableName);

        Column col_id = ColumnTestSuite.newColumn("id", DataElement.Type.LONG, StringUtils.EMPTY, false);
        Column col_name = ColumnTestSuite.newColumn("name", DataElement.Type.STRING, StringUtils.EMPTY, false);
        Column col_val = ColumnTestSuite.newColumn("value", DataElement.Type.UDT, UDTTestSuite.c_SampleUDTName, true);
        Column col_addr = ColumnTestSuite.newColumn("addr", DataElement.Type.STRING, StringUtils.EMPTY, true);
        Column col_alias = ColumnTestSuite.newColumn("alias", DataElement.Type.STRING, StringUtils.EMPTY, false);

        table.addColumn(col_id)//
                        .addColumn(col_name)//
                        .addColumn(col_val)//
                        .addColumn(col_addr)//
                        .addColumn(col_alias)//
                        .addGroup((Group) new Group().addColumn(col_val).addColumn(col_name).setName(c_SampleGroupName_Val))//
                        .addGroup((Group) new Group().addColumn(col_addr).addColumn(col_alias).setName(c_SampleGroupName_Addr))//
                        .setPartitionKey((Key.Partition) new Key.Partition().addColumn(col_id))//
                        .setRowKey((Key.Row) new Key.Row().addColumn(col_id))//
                        .addClusteringKey((Key.Clustering) new Key.Clustering().addColumn(col_name).setName(c_SampleClusteringKeyName))//
        ;

        return table;
    }
}
