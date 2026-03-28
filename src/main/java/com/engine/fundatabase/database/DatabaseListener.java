package com.engine.fundatabase.database;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import com.engine.fundatabase.database.command.CreateTableCommand;
import com.engine.fundatabase.database.command.InsertCommand;
import com.engine.fundatabase.database.command.SQLCommand;
import com.engine.fundatabase.database.command.SelectCommand;
import com.engine.fundatabase.parser.SQL;

import sql.antlrfiles.SQLiteParser;
import sql.antlrfiles.SQLiteParserBaseListener;

public class DatabaseListener extends SQLiteParserBaseListener {

    private final Database database;
    private final List<SQLCommand> commands = new ArrayList<>();
    private Iterator<?> result;

    public DatabaseListener(Database database) {
        this.database = database;
    }

    @Override
    public void exitCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
        String tableName = sanitizeIdentifier(ctx.table_name().getText());

        Hashtable<String, String> colNameType = new Hashtable<>();
        Hashtable<String, String> colNameMin = new Hashtable<>();
        Hashtable<String, String> colNameMax = new Hashtable<>();

        String clusteringKey = null;
        for (SQLiteParser.Column_defContext columnDef : ctx.column_def()) {
            String columnName = sanitizeIdentifier(columnDef.column_name().getText());
            String columnType = columnDef.type_name() == null ? "TEXT" : columnDef.type_name().getText();
            colNameType.put(columnName, columnType);
            colNameMin.put(columnName, "");
            colNameMax.put(columnName, "");

            for (SQLiteParser.Column_constraintContext constraint : columnDef.column_constraint()) {
                String normalized = constraint.getText().toUpperCase();
                if (normalized.contains("PRIMARY") && normalized.contains("KEY")) {
                    clusteringKey = columnName;
                }
            }
        }

        if (clusteringKey == null && !ctx.column_def().isEmpty()) {
            clusteringKey = sanitizeIdentifier(ctx.column_def(0).column_name().getText());
        }

        commands.add(new CreateTableCommand(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
    }

    @Override
    public void exitInsert_stmt(SQLiteParser.Insert_stmtContext ctx) {
        String tableName = sanitizeIdentifier(ctx.table_name().getText());

        List<String> columns = new ArrayList<>();
        for (SQLiteParser.Column_nameContext column : ctx.column_name()) {
            columns.add(sanitizeIdentifier(column.getText()));
        }

        Hashtable<String, Object> values = new Hashtable<>();
        SQLiteParser.Select_stmtContext selectStmt = ctx.select_stmt();
        if (selectStmt != null && !selectStmt.select_core().isEmpty()) {
            SQLiteParser.Select_coreContext core = selectStmt.select_core(0);
            if (core.values_clause() != null && !core.values_clause().value_row().isEmpty()) {
                List<SQLiteParser.ExprContext> exprs = core.values_clause().value_row(0).expr();
                for (int i = 0; i < exprs.size(); i++) {
                    String columnName = columns.isEmpty() ? "col" + (i + 1) : columns.get(i);
                    values.put(columnName, parseValue(exprs.get(i).getText()));
                }
            }
        }

        commands.add(new InsertCommand(tableName, values));
    }

    @Override
    public void exitSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
        if (ctx.select_core().isEmpty()) {
            return;
        }

        SQLiteParser.Select_coreContext core = ctx.select_core(0);

        List<String> projectionColumns = new ArrayList<>();
        for (SQLiteParser.Result_columnContext resultColumn : core.result_column()) {
            projectionColumns.add(resultColumn.getText());
        }

        List<SQL> terms = new ArrayList<>();
        String[] operators = new String[0];

        if (core.where_expr != null) {
            SQL term = parseWhereExpression(core.where_expr, core);
            if (term != null) {
                terms.add(term);
            }
        }

        commands.add(new SelectCommand(projectionColumns, terms.toArray(new SQL[0]), operators));
    }

    private SQL parseWhereExpression(SQLiteParser.ExprContext exprContext, SQLiteParser.Select_coreContext core) {
        if (exprContext == null || exprContext.children == null || exprContext.children.size() < 3) {
            return null;
        }

        SQL term = new SQL();
        term._strTableName = core.join_clause() == null
                ? ""
                : sanitizeIdentifier(core.join_clause().table_or_subquery(0).table_name().getText());
        term._strColumnName = sanitizeIdentifier(exprContext.getChild(0).getText());
        term._strOperator = exprContext.getChild(1).getText();
        term._objValue = parseValue(exprContext.getChild(2).getText());

        return term;
    }

    private Object parseValue(String rawText) {
        String text = sanitizeLiteral(rawText);
        if (text.matches("^-?\\d+$")) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ignored) {
                return Long.parseLong(text);
            }
        }
        if (text.matches("^-?\\d+\\.\\d+$")) {
            return Double.parseDouble(text);
        }
        if ("NULL".equalsIgnoreCase(text)) {
            return null;
        }
        if ("TRUE".equalsIgnoreCase(text) || "FALSE".equalsIgnoreCase(text)) {
            return Boolean.parseBoolean(text.toLowerCase());
        }
        return text;
    }

    private String sanitizeIdentifier(String text) {
        return text.replace("`", "")
                .replace("\"", "")
                .replace("[", "")
                .replace("]", "");
    }

    private String sanitizeLiteral(String text) {
        String trimmed = text.trim();
        if ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    public Iterator<?> executeCommands() {
        for (SQLCommand command : commands) {
            Iterator<?> commandResult = command.execute(database);
            if (commandResult != null) {
                result = commandResult;
            }
        }
        return result;
    }
}
