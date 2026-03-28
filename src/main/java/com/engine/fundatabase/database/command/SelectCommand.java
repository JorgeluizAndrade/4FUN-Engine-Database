package com.engine.fundatabase.database.command;

import java.util.Iterator;
import java.util.List;

import com.engine.fundatabase.database.Database;
import com.engine.fundatabase.parser.SQL;

public class SelectCommand implements SQLCommand {

    private final List<String> projectionColumns;
    private final SQL[] whereTerms;
    private final String[] operators;

    public SelectCommand(List<String> projectionColumns, SQL[] whereTerms, String[] operators) {
        this.projectionColumns = projectionColumns;
        this.whereTerms = whereTerms;
        this.operators = operators;
    }

    public List<String> getProjectionColumns() {
        return projectionColumns;
    }

    @Override
    public Iterator<?> execute(Database database) {
        return database.selectFromTable(whereTerms, operators);
    }
}
