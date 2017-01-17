package net.yck.wrkdb.common.shared;

public interface IConfigurable {
    final static String THRIFT_DBSERVER_ENABLED = "thrift.dbserver.enabled";
    final static String THRIFT_DBSERVER_PORT = "thrift.dbserver.port";
    final static String THRIFT_MAX_WORKER_THREADS = "thrift.max.worker.threads";

    final static String AVRO_DBSERVER_ENABLED = "avro.dbserver.enabled";
    final static String AVRO_DBSERVER_PORT = "avro.dbserver.port";

    final static String DB_SYS_CATALOG_LOCATION = "db.sys.catalog.location";
    final static String DB_SYS_CATALOG_STORE_TYPE = "db.sys.catalog.store.type";

    final static String DB_APP_LOCATION = "db.app.location";
}
