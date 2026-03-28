package com.engine.fundatabase.database;

import java.util.Hashtable;
import java.util.Iterator;

import com.engine.fundatabase.parser.SQL;

public class Database implements IDatabase {

	@Override
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createIndex(String strTableName, String[] strarrColName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterator<?> selectFromTable(SQL[] arrSQLTerms, String[] strarrOperators) {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
