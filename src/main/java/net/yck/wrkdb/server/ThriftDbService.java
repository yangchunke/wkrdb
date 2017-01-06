package net.yck.wrkdb.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import net.yck.wrkdb.core.DBException;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;
import net.yck.wrkdb.service.thrift.DbService;
import net.yck.wrkdb.service.thrift.Key;
import net.yck.wrkdb.service.thrift.ServiceException;

class ThriftDbService implements DbService.Iface {

  private final ThriftDbServer server;
  private final Logger         LOG;

  public ThriftDbService(ThriftDbServer server, Logger LOG) {
    this.server = server;
    this.LOG = LOG;
  }

  @Override
  public String ping() throws TException {
    return server.getVersion();
  }

  @Override
  public String createOrUpdateCatalogFromJson(String catalogJson) throws ServiceException, TException {
    try {
      return server.dbManager.createOrUpdateCatalogFromJson(catalogJson);
    } catch (IOException | DBException e) {
      LOG.error(() -> e.getMessage());
      throw new ServiceException(e.getMessage());
    }
  }

  @Override
  public String createOrUpdateSchemaFromJson(String catalogName, String schemaJson)
      throws ServiceException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String createOrUpdateTableFromJson(String catalogName, String schemaName, String tableJson)
      throws ServiceException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> listOfCatalogs() throws ServiceException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> listOfSchemas(String catalogName) throws ServiceException, TException {
    try {
      return server.dbManager.listOfSchemas(catalogName);
    } catch (IOException | DBException e) {
      LOG.error(() -> e.getMessage());
      throw new ServiceException(e.getMessage());
    }
  }

  @Override
  public List<String> listOfTables(String catalogName, String schemaName) throws ServiceException, TException {
    try {
      return server.dbManager.listOfTables(catalogName, schemaName);
    } catch (IOException | DBException e) {
      LOG.error(() -> e.getMessage());
      throw new ServiceException(e.getMessage());
    }
  }

  @Override
  public DBContext createDBContext(String catalogName, String schemaName, String tableName,
      Map<String, String> properties) throws ServiceException, TException {
    return new DBContext(UUID.randomUUID().toString(), DBManager.buildIdentifier(catalogName, schemaName, tableName))//
        .setProperties(properties);
  }

  @Override
  public DBSchema getDBSchema(DBContext context) throws ServiceException, TException {
    try {
      return server.dbManager.getDBSchema(context);
    } catch (DBException e) {
      LOG.error(() -> e.getMessage());
      throw new ServiceException(e.getMessage());
    }
  }

  @Override
  public List<ByteBuffer> get(DBContext context, Key key, List<String> columns) throws ServiceException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int put(DBContext context, Key key, Map<String, ByteBuffer> mappings) throws ServiceException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int remove(DBContext context, Key key, List<String> columns) throws ServiceException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

}
