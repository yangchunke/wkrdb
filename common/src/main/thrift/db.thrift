namespace java net.yck.wrkdb.service.thrift

struct DBContext {
  1: required string uuid,
  2: required string identifier,
  3: optional map<string,string> properties,
}

struct DBSchema {
  1: required string partitionKeySchema,
  2: required string rowKeySchema,
  3: required map<string, string> columnGroupSchemaMap,
}

struct Key {
  1: required binary partition,
  2: optional binary row,
}
 
exception ServiceException {
  1: string message,
}
 
service DbService
{
  string ping();
  
  string createOrUpdateCatalogFromJson(
  1: string catalogJson
  ) throws (1:ServiceException ouch);
  
  string createOrUpdateSchemaFromJson(
  1: string catalogName,
  2: string schemaJson
  ) throws (1:ServiceException ouch);
  
  string createOrUpdateTableFromJson(
  1: string catalogName,
  2: string schemaName
  3: string tableJson
  ) throws (1:ServiceException ouch);

  list<string> listOfCatalogs() throws (1:ServiceException ouch);
  
  list<string> listOfSchemas(
  1: string catalogName
  ) throws (1:ServiceException ouch);
  
  list<string> listOfTables(
  1: string catalogName,
  2: string schemaName
  ) throws (1:ServiceException ouch);
  
  DBContext createDBContext(
  1: string catalogName,
  2: string schemaName
  3: string tableName,
  4: map<string, string> properties
  ) throws (1:ServiceException ouch);
  
  DBSchema getDBSchema(
  1: DBContext context
  ) throws (1:ServiceException ouch);

  list<binary> get(
  1: DBContext context, 
  2: Key key,
  3: list<string> columns
  ) throws (1:ServiceException ouch);

  i32 put(
  1: DBContext context, 
  2: Key key,
  3: map<string,binary> mappings
  ) throws (1:ServiceException ouch);

  i32 remove(
  1: DBContext context, 
  2: Key key,
  3: list<string> columns
  ) throws (1:ServiceException ouch);  
}
