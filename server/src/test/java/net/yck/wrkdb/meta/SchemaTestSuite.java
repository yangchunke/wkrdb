package net.yck.wrkdb.meta;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.common.util.AvroUtil;
import net.yck.wrkdb.common.util.JsonUtil;
import net.yck.wrkdb.meta.Group;
import net.yck.wrkdb.meta.Key;
import net.yck.wrkdb.meta.NamedElement;
import net.yck.wrkdb.meta.Schema;
import net.yck.wrkdb.meta.Table;
import net.yck.wrkdb.meta.UDT;

public class SchemaTestSuite extends MetaTestSuiteBase {

  public final static String c_SampleSchemaName = "uax";

  @Test
  public void test() {
    for (int i = 0; i < c_MaxNumOfTables * c_MaxNumOfUDTs; i++) {
      Schema expected = i == 0 ? sampleSchema() : newSchema(rand);
      Schema actual = NamedElement.fromJson(expected.toString(), Schema.class);
      Assert.assertEquals(expected, actual);
      Assert.assertEquals(expected.toAvro(), actual.toAvro());
    }
  }

  @Test
  public void testAvroE2E() {
    Schema schema = sampleSchema();

    try {
      LOG.info(JsonUtil.prettify(schema.toString()));
    } catch (Exception e1) {
      Assert.fail(schema.toAvro());
    }

    LOG.info("=======================================");

    try {
      LOG.info(JsonUtil.prettify(schema.toAvro()));
    } catch (Exception e1) {
      Assert.fail(schema.toAvro());
    }

    LOG.info("=======================================");

    Table table = schema.getTable(TableTestSuite.c_SampleTableName);
    org.apache.avro.Schema schemaAvroSchema =
        new org.apache.avro.Schema.Parser().parse(schema.toAvro());

    Key.Partition pKey = table.getPartitionKey();
    GenericRecord pkRec = new GenericData.Record(pKey.getScopedAvroSchema(schemaAvroSchema));
    pkRec.put("id", 1L);

    Key.Row rowKey = table.getRowKey();
    GenericRecord rkRec = new GenericData.Record(rowKey.getScopedAvroSchema(schemaAvroSchema));
    rkRec.put("id", 1L);

    Group grp = table.getGroup(TableTestSuite.c_SampleGroupName_Val);
    GenericRecord rec = new GenericData.Record(grp.getScopedAvroSchema(schemaAvroSchema));
    rec.put("name", "foo");

    UDT udt = schema.getUDT(UDTTestSuite.c_SampleUDTName);
    GenericRecord fld = new GenericData.Record(udt.getScopedAvroSchema(schemaAvroSchema));
    fld.put("left", "Left");
    fld.put("right", System.currentTimeMillis());
    rec.put("value", fld);

    try {
      String json = AvroUtil.toJson(rec, schemaAvroSchema);
      LOG.info(JsonUtil.prettify(json));
      GenericRecord actual = AvroUtil.fromJson(json, schemaAvroSchema);
      Assert.assertEquals(rec, actual);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    try {
      byte[] bytes = AvroUtil.toBytes(rec, schemaAvroSchema);
      GenericRecord actual = AvroUtil.fromBytes(bytes, schemaAvroSchema);
      Assert.assertEquals(rec, actual);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  static Schema newSchema(Random rand) {
    Schema schema = (Schema) new Schema()//
        .setVersion(rand == null ? StringUtils.EMPTY : Integer.toString(rand.nextInt()))//
        .setName(UUID.randomUUID().toString());

    final int numOfUDTs = rand == null ? 1 : rand.nextInt(c_MaxNumOfUDTs);
    for (int i = 0; i < numOfUDTs; i++) {
      schema.addUDT(UDTTestSuite.newUDT(schema, rand));
    }

    final int numOfTables = rand == null ? 1 : Math.max(1, rand.nextInt(c_MaxNumOfTables));
    for (int i = 0; i < numOfTables; i++) {
      schema.addTable(TableTestSuite.newTable(schema, rand));
    }

    return schema;
  }

  static Schema sampleSchema() {
    Schema schema = (Schema) new Schema()//
        .setVersion("0.1")//
        .setName(c_SampleSchemaName);

    schema.addUDT(UDTTestSuite.sampleUDT())//
        .addTable(TableTestSuite.sampleTable());

    return schema;
  }
}
