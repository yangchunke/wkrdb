package net.yck.wrkdb.server;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.yck.wrkdb.server.meta.CatalogTestSuite;
import net.yck.wrkdb.server.meta.SchemaTestSuite;
import net.yck.wrkdb.server.meta.TableTestSuite;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;

public class DBSvcTestSuite extends AppSelfService {

  @Before
  public void before() {
    try {
      getThriftDbService().createOrUpdateCatalogFromJson(CatalogTestSuite.sampleCatalog().toString());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfSchemas() {
    try {
      List<String> schemas = getThriftDbService().listOfSchemas(CatalogTestSuite.c_SampleCatalogName);
      LOG.info(schemas);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfTables() {
    try {
      List<String> tables =
          getThriftDbService().listOfTables(CatalogTestSuite.c_SampleCatalogName, SchemaTestSuite.c_SampleSchemaName);
      LOG.info(tables);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void createDBContext() {
    try {
      DBContext ctx = getThriftDbService().createDBContext(CatalogTestSuite.c_SampleCatalogName,
          SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName, null);
      LOG.info(ctx);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void getDBSchema() {
    try {
      DBContext ctx = getThriftDbService().createDBContext(CatalogTestSuite.c_SampleCatalogName,
          SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName, null);
      DBSchema schema = getThriftDbService().getDBSchema(ctx);
      LOG.info(schema);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
