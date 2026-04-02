package com.engine.fundatabase.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import com.engine.fundatabase.parser.SQL;
import com.engine.fundatabase.parser.SQLParser;
import com.engine.fundatabase.storage.Row;
import com.engine.fundatabase.storage.Table;
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
		// TODO Auto-generated method stub

		Table table = serializer.deserializeTable(strTableName);

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
		ArrayList<ArrayList<Row>> result = new ArrayList<>();
		for (int i = 0; i < arrSQLTerms.length; i++) {

			Hashtable<String, Object> colNameValue = new Hashtable<>();
			colNameValue.put(arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._objValue);
			
			System.out.println("TERM DEBUG -> "
				    + arrSQLTerms[i]._strColumnName + " "
				    + arrSQLTerms[i]._strOperator + " "
				    + arrSQLTerms[i]._objValue + " (" 
				    + arrSQLTerms[i]._objValue.getClass() + ")");

							
			result.add(selectFromTableHelper(arrSQLTerms[i]._strTableName, colNameValue, arrSQLTerms[i]._strOperator));
		}
		
		
		System.out.println("RESOLTE DO SELECT: " + result);
		return applyArrOperators(result, strarrOperators);
	}

	private static ArrayList<Row> applyArrOperators(ArrayList<ArrayList<Row>> selections, String[] strarrOperators) {

		if (selections == null || selections.isEmpty()) {
			System.out.println("Nenhuma seleção encontrada.");
			return new ArrayList<>();
		}

		ArrayList<Row> result = new ArrayList<>(selections.get(0));

		System.out.println("RESPONSE: " + result);
		return result;
	}

	private static ArrayList<Row> selectFromTableHelper(String _strTableName, Hashtable<String, Object> colNameValue,
			String _strOperator) {
		
		Table table = new Serializer("data").deserializeTable(_strTableName);
		System.out.println("DEBUG TABLE DATA: " + table);
		System.out.println("ROWS: " + table.getRow());

		return table.select(colNameValue, _strOperator);

	}

}
