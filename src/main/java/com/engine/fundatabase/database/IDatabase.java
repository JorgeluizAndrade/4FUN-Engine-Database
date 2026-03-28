package com.engine.fundatabase.database;

import java.util.Hashtable;
import java.util.Iterator;

import com.engine.fundatabase.parser.SQL;

public interface IDatabase {
	
	
	public Iterator<?> executeSQL(String sql);
	
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax);
	
	
	public void createIndex(String strTableName, String[] strarrColName);

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue);
	public Iterator<?> selectFromTable(SQL[] arrSQLTerms, String[] strarrOperators);

}
