package com.engine.fundatabase.database.command;

import java.util.Hashtable;
import java.util.Iterator;

import com.engine.fundatabase.database.Database;

public class CreateTableCommand implements SQLCommand {

    private final String tableName;
    private final String clusteringKeyColumn;
    private final Hashtable<String, String> colNameType;
    private final Hashtable<String, String> colNameMin;
    private final Hashtable<String, String> colNameMax;

    public CreateTableCommand(
            String tableName,
            String clusteringKeyColumn,
            Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin,
            Hashtable<String, String> colNameMax) {
        this.tableName = tableName;
        this.clusteringKeyColumn = clusteringKeyColumn;
        this.colNameType = colNameType;
        this.colNameMin = colNameMin;
        this.colNameMax = colNameMax;
    }

    @Override
    public Iterator<?> execute(Database database) {
        System.out.println("PASSEI AQUI CreateTableCommand(execute)");

        database.createTable(tableName, clusteringKeyColumn, colNameType, colNameMin, colNameMax);
        return null;
    }
}
