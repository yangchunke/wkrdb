package net.yck.wrkdb.server;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import avro.shaded.com.google.common.base.Joiner;
import net.yck.wkrdb.DB;
import net.yck.wrkdb.core.DBException;
import net.yck.wrkdb.core.DBOptions;
import net.yck.wrkdb.meta.Catalog;
import net.yck.wrkdb.meta.Schema;
import net.yck.wrkdb.service.thrift.DBContext;
import net.yck.wrkdb.service.thrift.DBSchema;
import net.yck.wrkdb.shared.AppBase;
import net.yck.wrkdb.shared.AppBase.Component;
import net.yck.wrkdb.store.GetOptions;
import net.yck.wrkdb.store.PutOptions;
import net.yck.wrkdb.store.Store;

class DBManager extends Component implements AutoCloseable {

  final static Logger         LOG       = LogManager.getLogger(DBManager.class);

  final static String         c_SvcDir  = Paths.get(System.getProperty("java.io.tmpdir"), "tedb").toString();

  private final static String c_Schema  = "sys";
  private final static String c_Table   = "catalog";
  private final static String c_Group   = "Val";

  DB                          sysDB;
  Map<String, DB>             appDBs    = new ConcurrentHashMap<String, DB>();
  Map<String, Store>          appStores = new ConcurrentHashMap<String, Store>();

  public DBManager(AppBase app) {
    super(app);
  }

  @Override
  public Component initialize() throws Exception {

    Catalog catalog = Catalog.fromResource(DB.class, "/catalogs/sys.json");

    DBOptions options = new DBOptions()//
        .setRootPath(app.config.getProperty(DB_SYS_CATALOG_LOCATION, c_SvcDir))//
        .setStoreType(app.config.getProperty(DB_SYS_CATALOG_STORE_TYPE, Store.Type.MapDB))//
        .setNumOfShards(1);

    sysDB = DB.builder()//
        .setCatalog(catalog)//
        .setOptions(options)//
        .build();

    return this;
  }

  @Override
  public void close() throws Exception {
    if (sysDB != null) {
      sysDB.close();
      sysDB = null;
    }
    for (DB db : appDBs.values()) {
      db.close();
    }
    appDBs.clear();
  }

  static String buildIdentifier(String catalogName, String schemaName, String tableName) {
    return Joiner.on(':').join(catalogName, schemaName, tableName);
  }

  static ImmutableTriple<String, String, String> parseIdentifier(String identifier) {
    String[] arr = identifier.split(":");
    return new ImmutableTriple<String, String, String>(arr[0], arr[1], arr[2]);
  }

  String createOrUpdateCatalogFromJson(String catalogJson) throws IOException, DBException {
    Catalog newCatalog = Catalog.fromJson(catalogJson);
    Catalog oldCatalog = getCatalog(newCatalog.getName());

    boolean operate = false;
    if (oldCatalog == null) {
      LOG.info(() -> "create catalog [" + newCatalog.getName() + "].");
      operate = true;
    } else if (newCatalog.getVersion().compareTo(oldCatalog.getVersion()) > 0) {
      LOG.info(() -> "update catalog [" + newCatalog.getName() + "] from " + oldCatalog.getVersion() + " to "
          + newCatalog.getVersion());
      operate = true;
    }

    if (operate) {
      Store store = sysDB.getStore(c_Schema, c_Table);
      org.apache.avro.Schema rowKeyAvroSchema = store.getRowKeyAvroSchema();
      Map<String, org.apache.avro.Schema> groupAvroSchemaMap = store.getGroupAvroSchemaMap();

      GenericRecord rowKey = new GenericData.Record(rowKeyAvroSchema);
      rowKey.put("name", newCatalog.getName());

      GenericRecord val = new GenericData.Record(groupAvroSchemaMap.get(c_Group));
      val.put("json", catalogJson);

      store.put(PutOptions.c_default, rowKey, new HashMap<String, GenericRecord>() {
        private static final long serialVersionUID = -7228975586475145637L;
        {
          put(c_Group, val);
        }
      });
    }

    return newCatalog.toAvro();
  }

  List<String> listOfSchemas(String catalogName) throws DBException, IOException {
    Catalog catalog = getCatalog(catalogName);
    if (catalog == null) {
      throw new DBException("catalog [" + catalogName + "] doesn't exist.");
    }
    return catalog.getSchemas().stream().map(x -> x.getName()).collect(Collectors.toList());
  }

  List<String> listOfTables(String catalogName, String schemaName) throws DBException, IOException {
    Catalog catalog = getCatalog(catalogName);
    if (catalog == null) {
      throw new DBException("catalog [" + catalogName + "] doesn't exist.");
    }
    Schema schema = catalog.getSchema(schemaName);
    if (schema == null) {
      throw new DBException("schema [" + schemaName + "] doesn't exist in catalog [" + catalogName + "].");
    }
    return schema.getTables().stream().map(x -> x.getName()).collect(Collectors.toList());
  }

  DBSchema getDBSchema(DBContext context) throws DBException {
    Store store = appStores.computeIfAbsent(context.identifier, identifier -> {
      Store _store = null;
      try {
        ImmutableTriple<String, String, String> triple = parseIdentifier(identifier);
        DB db = getDB(triple.left);
        _store = db.getStore(triple.middle, triple.right);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
      return _store;
    });

    if (store == null) {
      throw new DBException("unable to obtain store for " + context);
    }

    Map<String, String> groupSchemas = new HashMap<>(store.getGroupAvroSchemaMap().size());
    for (Map.Entry<String, org.apache.avro.Schema> entry : store.getGroupAvroSchemaMap().entrySet()) {
      groupSchemas.put(entry.getKey(), entry.getValue().toString());
    }
    return new DBSchema(store.getPartitionKeyAvroSchema().toString(), store.getRowKeyAvroSchema().toString(),
        groupSchemas);
  }

  private DB getDB(String catalogName) {
    return appDBs.computeIfAbsent(catalogName, name -> {
      DB db = null;
      try {
        Catalog catalog = getCatalog(name);
        DBOptions options = new DBOptions()//
            .setRootPath(app.config.getProperty(DB_APP_LOCATION, c_SvcDir))//
            .setStoreType(Store.Type.MapDB)//
            .setNumOfShards(1);
        db = DB.builder().setCatalog(catalog).setOptions(options).build();
      } catch (DBException | IOException e) {
        LOG.error(e.getMessage());
      }
      return db;
    });
  }

  private Catalog getCatalog(String catalogName) throws DBException, IOException {
    Store store = sysDB.getStore(c_Schema, c_Table);

    org.apache.avro.Schema rowKeyAvroSchema = store.getRowKeyAvroSchema();
    GenericRecord rowKey = new GenericData.Record(rowKeyAvroSchema);
    rowKey.put("name", catalogName);

    List<GenericRecord> vals = store.get(GetOptions.c_default, rowKey, Arrays.asList(c_Group));
    GenericRecord val = vals.get(0);
    return val == null ? null : Catalog.fromJson(val.get("json").toString());
  }
}
