package net.yck.wrkdb.server.db;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import net.yck.wkrdb.server.db.DB;
import net.yck.wkrdb.server.db.DBOptions;
import net.yck.wkrdb.server.db.DBState;
import net.yck.wrkdb.server.meta.CatalogTestSuite;
import net.yck.wrkdb.server.meta.SchemaTestSuite;
import net.yck.wrkdb.server.meta.TableTestSuite;
import net.yck.wrkdb.server.store.Store;

public class DBTestSuite implements ITestSuite {

    @Test
    public void test() {
        try (DB db = DB.builder()//
                        .setCatalog(CatalogTestSuite.sampleCatalog())//
                        .setOptions(new DBOptions().setRootPath(c_TmpDir).setStoreType(Store.Type.MapDB)).build()) {
            Store store = db.getStore(SchemaTestSuite.c_SampleSchemaName, TableTestSuite.c_SampleTableName);
            Assert.assertEquals(DBState.Open, store.getState());
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

}
