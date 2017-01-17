package net.yck.wrkdb.server.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wkrdb.server.db.DB;
import net.yck.wkrdb.server.db.DBOptions;
import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.server.db.ITestSuite;
import net.yck.wrkdb.server.meta.CatalogTestSuite;
import net.yck.wrkdb.server.meta.SchemaTestSuite;
import net.yck.wrkdb.server.meta.TableTestSuite;
import net.yck.wrkdb.server.meta.UDT;
import net.yck.wrkdb.server.meta.UDTTestSuite;
import net.yck.wrkdb.server.store.GetOptions;
import net.yck.wrkdb.server.store.PutOptions;
import net.yck.wrkdb.server.store.Store;
import net.yck.wrkdb.server.util.Sharder;

public class MapDBStoreTestSuite extends StoreTestSuiteBase {

  private static DBOptions c_defaults = new DBOptions()//
      .setRootPath(c_TmpDir)//
      .setStoreType(Store.Type.MapDB)//
      .setNumOfShards(7);

  @Test
  public void test() {
    try {

      DBOptions options = new DBOptions(c_defaults);
      options.setProperty(Sharder.Prop_SharderType, Sharder.Type.Murmur3.name());

      DB db = DB.builder().setCatalog(CatalogTestSuite.sampleCatalog()).setOptions(options).build();

      Store store =
          db.getStore(SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName);

      org.apache.avro.Schema rowKeyAvroSchema = store.getRowKeyAvroSchema();
      LOG.info(rowKeyAvroSchema);

      Map<String, org.apache.avro.Schema> groupAvroSchemaMap = store.getGroupAvroSchemaMap();
      for (org.apache.avro.Schema schema : groupAvroSchemaMap.values()) {
        LOG.info(schema);
      }

      boolean ret = ITestSuite.executeTasksAndWait(() -> new TestRunner(store), "MapDBStoreTestSuite",
          Runtime.getRuntime().availableProcessors(), 10L, TimeUnit.SECONDS, LOG);

      store.close();
      Assert.assertTrue(ret);
    } catch (DBException e) {
      Assert.fail(e.getMessage());
    }
  }

  private static class TestRunner implements Runnable {

    final Store store;
    final org.apache.avro.Schema rowKeyAvroSchema;
    final Map<String, org.apache.avro.Schema> groupAvroSchemaMap;

    TestRunner(Store store) {
      this.store = store;
      rowKeyAvroSchema = store.getRowKeyAvroSchema();
      groupAvroSchemaMap = store.getGroupAvroSchemaMap();
    }

    @Override
    public void run() {
      try {
        _run();
      } catch (DBException e) {
        Assert.fail(e.getMessage());
      }
    }

    private void _run() throws DBException {
      final List<String> columns = Arrays.asList(TableTestSuite.c_SampleGroupName_Val,
          TableTestSuite.c_SampleGroupName_Addr);
      for (int i = 0; i < c_Iteration * c_Iteration; i++) {
        GenericRecord rkRec = new GenericData.Record(rowKeyAvroSchema);
        rkRec.put("id", rand.nextLong());

        GenericRecord val_expected =
            new GenericData.Record(groupAvroSchemaMap.get(TableTestSuite.c_SampleGroupName_Val));
        val_expected.put("name", UUID.randomUUID().toString());

        if (rand.nextBoolean()) {
          UDT udt = CatalogTestSuite.sampleCatalog().getSchema(SchemaTestSuite.c_SampleSchemaName)
              .getUDT(UDTTestSuite.c_SampleUDTName);
          GenericRecord fld =
              new GenericData.Record(udt.getScopedAvroSchema(store.getSchemaAvroSchema()));
          fld.put("left", UUID.randomUUID().toString());
          fld.put("right", System.currentTimeMillis());
          val_expected.put("value", fld);
        }

        GenericRecord addr_expected =
            new GenericData.Record(groupAvroSchemaMap.get(TableTestSuite.c_SampleGroupName_Addr));
        addr_expected.put("addr", UUID.randomUUID().toString());
        addr_expected.put("alias", UUID.randomUUID().toString());

        store.put(PutOptions.c_default, rkRec, new HashMap<String, GenericRecord>() {
          private static final long serialVersionUID = 8222861411022123416L;
          {
            put(TableTestSuite.c_SampleGroupName_Val, val_expected);
            put(TableTestSuite.c_SampleGroupName_Addr, addr_expected);
          }
        });

        {
          List<GenericRecord> actuals = store.get(GetOptions.c_default, rkRec, columns);
          Assert.assertEquals(val_expected, actuals.get(0));
          Assert.assertEquals(addr_expected, actuals.get(1));
        }

        store.remove(PutOptions.c_default, rkRec, columns);

        {
          List<GenericRecord> actual = store.get(GetOptions.c_default, rkRec, columns);
          Assert.assertEquals(null, actual.get(0));
          Assert.assertEquals(null, actual.get(1));
        }
      }
    }
  }
}
