package net.yck.wrkdb.store;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

import net.yck.wrkdb.common.DBException;
import net.yck.wrkdb.meta.Table;

abstract class FileStore extends Store {
    protected FileStore(Table table) {
        super(table);
    }

    protected String folder() throws DBException {
        Path dir = Paths.get(getOptions().getRootPath(), identifier(File.separatorChar));
        if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
            try {
                dir.toFile().mkdirs();
            }
            catch (Exception e) {
                throw new DBException(e);
            }
        }
        return dir.toString();
    }
}
