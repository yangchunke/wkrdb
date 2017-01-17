package net.yck.wrkdb.core;

import java.util.Map;
import java.util.Properties;

import net.yck.wrkdb.store.Store;

public class DBOptions extends Properties {

  private static final long    serialVersionUID       = 8368196885461248493L;

  private static final String  Prop_DB_rootPath       = "db.rootPath";
  private static final String  Prop_Store_Type        = "store.Type";
  private static final String  Prop_Store_NumOfShards = "store.numOfShards";
  private static final String  Prop_Store_UseCache    = "store.useCache";

  private final static int     def_NumOfShards        = 11;
  private final static boolean def_useCache           = true;

  public DBOptions() {
    super();
  }

  public DBOptions(DBOptions defaults) {
    super(defaults);
  }

  public String getRootPath() {
    return this.getProperty(Prop_DB_rootPath);
  }

  @SuppressWarnings("unchecked")
  public <T extends DBOptions> T setRootPath(String rootPath) {
    this.setProperty(Prop_DB_rootPath, rootPath);
    return (T) this;
  }

  public Store.Type getStoreType() {
    return Store.Type.valueOf(this.getProperty(Prop_Store_Type));
  }

  @SuppressWarnings("unchecked")
  public <T extends DBOptions> T setStoreType(Store.Type storeType) {
    this.setProperty(Prop_Store_Type, storeType.name());
    return (T) this;
  }

  public int getNumOfShards() {
    return Integer.parseInt(this.getProperty(Prop_Store_NumOfShards, Integer.toString(def_NumOfShards)));
  }

  @SuppressWarnings("unchecked")
  public <T extends DBOptions> T setNumOfShards(int numOfShards) {
    this.setProperty(Prop_Store_NumOfShards, Integer.toString(numOfShards));
    return (T) this;
  }

  public boolean getUseCache() {
    return Boolean.parseBoolean(this.getProperty(Prop_Store_UseCache, Boolean.toString(def_useCache)));
  }

  @SuppressWarnings("unchecked")
  public <T extends DBOptions> T setUseCache(boolean useCache) {
    this.setProperty(Prop_Store_UseCache, Boolean.toString(useCache));
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  protected <T extends DBOptions> T populate(Map<String, String> prop) {
    prop.forEach((key, val) -> put(key, val));
    return (T) this;
  }

}