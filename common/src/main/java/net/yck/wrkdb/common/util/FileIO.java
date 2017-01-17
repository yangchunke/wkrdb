package net.yck.wrkdb.common.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public final class FileIO {

    private static String SUFFIX_GZIPPED = ".gz";

    public final static BufferedReader getBufferedReader(String file, Charset charset) throws FileNotFoundException, IOException {
        Preconditions.checkArgument(!StringUtils.isEmpty(file));
        InputStream fis = uncompressed(new FileInputStream(file), file);
        return new BufferedReader(new InputStreamReader(fis, charset));
    }

    public final static BufferedReader getBufferedReader(String file) throws FileNotFoundException, IOException {
        return getBufferedReader(file, StandardCharsets.UTF_8);
    }

    final static InputStream uncompressed(InputStream raw, String file) throws IOException {
        if (file.toLowerCase().endsWith(SUFFIX_GZIPPED)) {
            return new GZIPInputStream(raw);
        }
        return raw;
    }
}
