package com.mesosphere.dcos.cassandra.common.util;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Util methods for File
 */
public class FileUtil {
    public static void deleteDirectory(File file) throws FileNotFoundException {
        if (file != null && file.isDirectory()) {
            for (File f : file.listFiles())
                deleteDirectory(f);
        }
        if (file != null && !file.delete())
            throw new FileNotFoundException("Failed to delete file: " + file);
    }
}
