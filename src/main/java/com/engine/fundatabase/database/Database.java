package com.engine.fundatabase.database;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import com.engine.fundatabase.parser.SQL;
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
		
		// fazer writer e reader of json...
		
		
		System.out.println("DEBUG TABLE NAME: " + strTableName);

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
		
	}
	
	
	private void insertRow(Table table, Hashtable<String, Object> htblColNameValue) {
		table.insertRow(htblColNameValue);	
	}

	@Override
	public Iterator<?> selectFromTable(SQL[] arrSQLTerms, String[] strarrOperators) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<?> executeSQL(String sql) {
	    SQLParser parser = new SQLParser(this);
	    return parser.parse(sql);
	
	}

	
	
}
