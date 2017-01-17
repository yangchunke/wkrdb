package net.yck.wrkdb.server;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.yck.wrkdb.meta.CatalogTestSuite;
import net.yck.wrkdb.meta.SchemaTestSuite;
import net.yck.wrkdb.meta.TableTestSuite;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;

public class DBSvcTestSuite extends AppSelfService {

  @Before
  public void before() {
    try {
      getThriftIface().createOrUpdateCatalogFromJson(CatalogTestSuite.sampleCatalog().toString());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfSchemas() {
    try {
      List<String> schemas = getThriftIface().listOfSchemas(CatalogTestSuite.c_SampleCatalogName);
      LOG.info(schemas);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfTables() {
    try {
      List<String> tables =
          getThriftIface().listOfTables(CatalogTestSuite.c_SampleCatalogName, SchemaTestSuite.c_SampleSchemaName);
      LOG.info(tables);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void createDBContext() {
    try {
      DBContext ctx = getThriftIface().createDBContext(CatalogTestSuite.c_SampleCatalogName,
          SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName, null);
      LOG.info(ctx);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void getDBSchema() {
    try {
      DBContext ctx = getThriftIface().createDBContext(CatalogTestSuite.c_SampleCatalogName,
          SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName, null);
      DBSchema schema = getThriftIface().getDBSchema(ctx);
      LOG.info(schema);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
