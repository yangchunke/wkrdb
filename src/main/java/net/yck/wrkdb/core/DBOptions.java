package net.yck.wrkdb.core;

import java.util.Properties;

import net.yck.wrkdb.store.Store;

public class DBOptions extends Properties {

    private static final long serialVersionUID = 8368196885461248493L;

    private static final String Prop_DB_rootPath = "db.rootPath";
    private static final String Prop_Store_Type = "store.Type";
    private static final String Prop_Store_NumOfShards = "store.numOfShard";

    private final static int def_NumOfShards = 11;

    public DBOptions() {
        super();
    }

    public DBOptions(DBOptions defaults) {
        super(defaults);
    }

    public String getRootPath() {
        return this.getProperty(Prop_DB_rootPath);
    }

    public DBOptions setRootPath(String rootPath) {
        this.setProperty(Prop_DB_rootPath, rootPath);
        return this;
    }

    public Store.Type getStoreType() {
        return Store.Type.valueOf(this.getProperty(Prop_Store_Type));
    }

    public DBOptions setStoreType(Store.Type storeType) {
        this.setProperty(Prop_Store_Type, storeType.name());
        return this;
    }

    public int getNumOfShards() {
        return Integer.parseInt(this.getProperty(Prop_Store_NumOfShards, Integer.toString(def_NumOfShards)));
    }

    public DBOptions setNumOfShards(int numOfShards) {
        this.setProperty(Prop_Store_NumOfShards, Integer.toString(numOfShards));
        return this;
    }

}
