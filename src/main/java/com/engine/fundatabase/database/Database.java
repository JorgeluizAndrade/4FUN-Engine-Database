package com.engine.fundatabase.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import com.engine.fundatabase.parser.SQL;
import com.engine.fundatabase.parser.SQLParser;
import com.engine.fundatabase.storage.Row;
import com.engine.fundatabase.storage.Table;
import com.engine.fundatabase.utils.Constants;
import com.engine.fundatabase.utils.serializer.Serializer;

public class Database implements IDatabase {

	private HashSet<String> myTables;
	private Serializer serializer;

	public Database() {
		this.serializer = new Serializer("data");
		this.myTables = new HashSet<String>();
	}

	@Override
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) {
		// TODO Auto-generated method stub
		Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
		
		System.out.println("PASSEI AQUI: CREATE TABLE -> " + strTableName);

		myTables.add(strTableName);

		serializer.serializeTable(table);

	}

	@Override
	public void createIndex(String strTableName, String[] strarrColName) {
		// TODO Auto-generated method stub

	}

	@Override
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
		if (strTableName == null || strTableName.trim().isEmpty()) {
			throw new IllegalArgumentException("Table name cannot be null or empty.");
		}
		if (htblColNameValue == null || htblColNameValue.isEmpty()) {
			throw new IllegalArgumentException("Inserted values cannot be null or empty.");
		}

		Table table = serializer.deserializeTable(strTableName);
		if (table == null) {
			throw new IllegalArgumentException("Table not found: " + strTableName);
		}

		System.out.println("table name: " + strTableName + "\n col name" + htblColNameValue);

		insertRow(table, htblColNameValue);
		
		serializer.serializeTable(table);
	}

	private void insertRow(Table table, Hashtable<String, Object> htblColNameValue) {
		table.insertRow(htblColNameValue);
	}

	@Override
	public Iterator<?> selectFromTable(SQL[] arrSQLTerms, String[] strarrOperators) {
		// TODO Auto-generated method stub
		System.out.println("PASSEI AQUI DATABASE: SELECT  " + arrSQLTerms.toString() + " \n" + strarrOperators.toString());
		return selectSimple(arrSQLTerms, strarrOperators).iterator();
	}

	@Override
	public Iterator<?> executeSQL(String sql) {
		SQLParser parser = new SQLParser(this);
		return parser.parse(sql);

	}

	private static ArrayList<Row> selectSimple(SQL[] arrSQLTerms, String[] strarrOperators) {
		if (arrSQLTerms == null || arrSQLTerms.length == 0) {
			return new ArrayList<>();
		}

		Table table = new Serializer("data").deserializeTable(arrSQLTerms[0]._strTableName);
		ArrayList<ArrayList<Row>> result = new ArrayList<>();
		for (int i = 0; i < arrSQLTerms.length; i++) {

			Hashtable<String, Object> colNameValue = new Hashtable<>();
			colNameValue.put(arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._objValue);
			
			System.out.println("TERM DEBUG -> "
				    + arrSQLTerms[i]._strColumnName + " "
				    + arrSQLTerms[i]._strOperator + " "
				    + arrSQLTerms[i]._objValue + " (" 
				    + arrSQLTerms[i]._objValue.getClass() + ")");

							
			result.add(selectFromTableHelper(table, colNameValue, arrSQLTerms[i]._strOperator));
		}
		
		
		System.out.println("RESOLTE DO SELECT: " + result);
		return applyArrOperators(result, strarrOperators);
	}

	private static ArrayList<Row> applyArrOperators(ArrayList<ArrayList<Row>> selections, String[] strarrOperators) {

		if (selections == null || selections.isEmpty()) {
			return new ArrayList<>();
		}
		if (selections.size() == 1) {
			return new ArrayList<>(selections.get(0));
		}
		if (strarrOperators == null || strarrOperators.length != selections.size() - 1) {
			throw new IllegalArgumentException("Quantidade de operadores inválida para os termos do WHERE.");
		}

		ArrayList<Row> result = new ArrayList<>(selections.get(0));
		for (int i = 0; i < strarrOperators.length; i++) {
			result = applyOperator(result, selections.get(i + 1), strarrOperators[i]);
		}
		return result;
	}

	private static ArrayList<Row> applyOperator(ArrayList<Row> leftSelection, ArrayList<Row> rightSelection, String operator) {
		String normalizedOperator = operator == null ? "" : operator.trim().toLowerCase(Locale.ROOT);

		Map<Object, Row> leftRows = mapByPrimaryKey(leftSelection);
		Map<Object, Row> rightRows = mapByPrimaryKey(rightSelection);

		if (Constants.AND_OPERATION.equals(normalizedOperator)) {
			ArrayList<Row> intersection = new ArrayList<>();
			for (Map.Entry<Object, Row> entry : leftRows.entrySet()) {
				if (rightRows.containsKey(entry.getKey())) {
					intersection.add(entry.getValue());
				}
			}
			return intersection;
		}

		if (Constants.OR_OPERATION.equals(normalizedOperator)) {
			LinkedHashMap<Object, Row> union = new LinkedHashMap<>(leftRows);
			union.putAll(rightRows);
			return new ArrayList<>(union.values());
		}

		if (Constants.XOR_OPERATION.equals(normalizedOperator)) {
			LinkedHashMap<Object, Row> xor = new LinkedHashMap<>();
			for (Map.Entry<Object, Row> entry : leftRows.entrySet()) {
				if (!rightRows.containsKey(entry.getKey())) {
					xor.put(entry.getKey(), entry.getValue());
				}
			}
			for (Map.Entry<Object, Row> entry : rightRows.entrySet()) {
				if (!leftRows.containsKey(entry.getKey())) {
					xor.put(entry.getKey(), entry.getValue());
				}
			}
			return new ArrayList<>(xor.values());
		}

		throw new IllegalArgumentException("Operador lógico não suportado: " + operator);
	}

	private static Map<Object, Row> mapByPrimaryKey(ArrayList<Row> rows) {
		LinkedHashMap<Object, Row> mappedRows = new LinkedHashMap<>();
		for (Row row : rows) {
			mappedRows.put(row.getPrimaryKey(), row);
		}
		return mappedRows;
	}

	private static ArrayList<Row> selectFromTableHelper(Table table, Hashtable<String, Object> colNameValue,
			String _strOperator) {
		return table.select(colNameValue, _strOperator);

	}

}
