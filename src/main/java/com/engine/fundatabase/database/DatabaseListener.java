package com.engine.fundatabase.database;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.engine.fundatabase.database.command.CreateTableCommand;
import com.engine.fundatabase.database.command.InsertCommand;
import com.engine.fundatabase.database.command.SQLCommand;
import com.engine.fundatabase.database.command.SelectCommand;
import com.engine.fundatabase.parser.SQL;
import funengine.sql.SQLiteParser;
import funengine.sql.SQLiteParserBaseListener;


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
				populateConstraintBounds(columnName, constraint.getText(), colNameMin, colNameMax);
			}
		}

		if (clusteringKey == null && !ctx.column_def().isEmpty()) {
			clusteringKey = sanitizeIdentifier(ctx.column_def(0).column_name().getText());
		}

		System.out.println("PASSEI AQUI DatabaseListener");

		System.out.println("clusteringKey: " + clusteringKey + "\n" + "table name: " + tableName);

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
        System.out.println("PASSEI AQUI DatabaseListener -> SELECT");

        SQLiteParser.Select_coreContext core = ctx.select_core(0);
        String tableName = resolveTableName(core);
        if (tableName.isBlank()) {
            // Ignora SELECTs internos usados pelo parse de INSERT ... VALUES.
            return;
        }

        List<String> projectionColumns = new ArrayList<>();
        for (SQLiteParser.Result_columnContext resultColumn : core.result_column()) {
            projectionColumns.add(resultColumn.getText());
        }

        List<SQL> terms = buildWhereTerms(core.where_expr, tableName);
        String[] operators = buildLogicalOperators(core.where_expr);
        System.out.println("NO LISTENER(SELECT) AINDA : " + projectionColumns);
        

        commands.add(new SelectCommand(projectionColumns, terms.toArray(new SQL[0]), operators));
    }
		
	
	private List<SQL> buildWhereTerms(SQLiteParser.ExprContext exprContext, String tableName) {
        List<SQL> terms = new ArrayList<>();
        if (exprContext == null) {
            return terms;
        }
        Matcher matcher = Pattern
                .compile("(?i)([\\w.`\"\\[\\]]+)\\s*(>=|<=|!=|=|>|<)\\s*('[^']*'|\"[^\"]*\"|[^\\s]+)")
                .matcher(exprContext.getText());
        while (matcher.find()) {
            SQL term = new SQL();
            term._strTableName = tableName;
            term._strColumnName = sanitizeIdentifier(extractColumnName(matcher.group(1)));
            term._strOperator = matcher.group(2);
            term._objValue = parseValue(matcher.group(3));
            terms.add(term);
        }
        return terms;
    }

	private String resolveTableName(SQLiteParser.Select_coreContext core) {
		if (core.join_clause() != null && !core.join_clause().table_or_subquery().isEmpty()) {
			SQLiteParser.Table_or_subqueryContext tableContext = core.join_clause().table_or_subquery(0);
			if (tableContext.table_name() != null) {
				return sanitizeIdentifier(tableContext.table_name().getText());
			}
		}
		String statementText = core.getText();
		Matcher matcher = Pattern.compile("(?i)from([\\w`\"\\[\\]]+)").matcher(statementText);
		if (matcher.find()) {
			return sanitizeIdentifier(matcher.group(1));
		}
		return "";
	}

	private String[] buildLogicalOperators(SQLiteParser.ExprContext exprContext) {
		if (exprContext == null) {
			return new String[0];
		}
		List<String> operators = new ArrayList<>();
		Matcher matcher = Pattern.compile("(?i)(AND|OR|XOR)").matcher(exprContext.getText());
		while (matcher.find()) {
			operators.add(matcher.group(1));
		}
		return operators.toArray(new String[0]);
	}

	private String extractColumnName(String rawColumnName) {
		int lastDot = rawColumnName.lastIndexOf('.');
		if (lastDot >= 0 && lastDot + 1 < rawColumnName.length()) {
			return rawColumnName.substring(lastDot + 1);
		}
		return rawColumnName;
	}

	private void populateConstraintBounds(String columnName, String rawConstraint,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {
		String sanitized = rawConstraint.replaceAll("\\s+", "");
		Matcher minMatcher = Pattern.compile("(?i)" + Pattern.quote(columnName) + ">=([^\\)]+)").matcher(sanitized);
		if (minMatcher.find()) {
			colNameMin.put(columnName, sanitizeLiteral(minMatcher.group(1)));
		}
		Matcher maxMatcher = Pattern.compile("(?i)" + Pattern.quote(columnName) + "<=([^\\)]+)").matcher(sanitized);
		if (maxMatcher.find()) {
			colNameMax.put(columnName, sanitizeLiteral(maxMatcher.group(1)));
		}
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
		return text.replace("`", "").replace("\"", "").replace("[", "").replace("]", "");
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
