package net.yck.wrkdb.server;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.yck.wrkdb.client.ThriftDbClient;
import net.yck.wrkdb.meta.CatalogTestSuite;
import net.yck.wrkdb.meta.SchemaTestSuite;
import net.yck.wrkdb.meta.TableTestSuite;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;

public class DBSvcTestSuite extends AppSelfService {

  @Before
  public void before() {
    try (ThriftDbClient client = getThriftDbClient()) {
      client.createOrUpdateCatalogFromJson(CatalogTestSuite.sampleCatalog().toString());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfSchemas() {
    try (ThriftDbClient client = getThriftDbClient()) {
      List<String> schemas = client.listOfSchemas(CatalogTestSuite.c_SampleCatalogName);
      LOG.info(schemas);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void listOfTables() {
    try (ThriftDbClient client = getThriftDbClient()) {
      List<String> tables =
          client.listOfTables(CatalogTestSuite.c_SampleCatalogName, SchemaTestSuite.c_SampleSchemaName);
      LOG.info(tables);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void createDBContext() {
    try (ThriftDbClient client = getThriftDbClient()) {
      DBContext ctx = client.createDBContext(CatalogTestSuite.c_SampleCatalogName, SchemaTestSuite.c_SampleSchemaName,
          TableTestSuite.c_SampleTableName, null);
      LOG.info(ctx);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void getDBSchema() {
    try (ThriftDbClient client = getThriftDbClient()) {
      DBContext ctx = client.createDBContext(CatalogTestSuite.c_SampleCatalogName, SchemaTestSuite.c_SampleSchemaName,
          TableTestSuite.c_SampleTableName, null);
      DBSchema schema = client.getDBSchema(ctx);
      LOG.info(schema);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
