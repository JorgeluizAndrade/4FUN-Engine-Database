package com.engine.fundatabase.utils.serializer;

import java.io.File;

import com.engine.fundatabase.utils.Constants;

public class FileManager {

    private final String baseDir;

    public FileManager(String baseDir) {
        this.baseDir = baseDir;
    }

    public String getTablePath(String tableName) {
        return getPath(tableName, tableName);
    }

    public String getPagePath(String tableName, String pageName) {
        return getPath(tableName, pageName);
    }

    public void ensureParentDirectoryExists(String path) {
        File file = new File(path);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
    }

    private String getPath(String tableName, String targetName) {
        return baseDir + File.separator
                + tableName + File.separator
                + targetName + Constants.DATA_EXTENSTION;
    }
}
