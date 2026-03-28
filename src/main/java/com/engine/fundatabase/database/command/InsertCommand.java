package com.engine.fundatabase.database.command;

import java.util.Hashtable;
import java.util.Iterator;

import com.engine.fundatabase.database.Database;

public class InsertCommand implements SQLCommand {

    private final String tableName;
    private final Hashtable<String, Object> values;

    public InsertCommand(String tableName, Hashtable<String, Object> values) {
        this.tableName = tableName;
        this.values = values;
    }

    @Override
    public Iterator<?> execute(Database database) {
        database.insertIntoTable(tableName, values);
        return null;
    }
}
