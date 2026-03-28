package com.engine.fundatabase.parser;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SQL {
	public String _strTableName;
	public String _strColumnName;
	public String _strOperator;
	public Object _objValue;

	@Override
	public String toString() {
		return "SQL{" + "_strTableName='" + _strTableName + '\'' + ", _strColumnName='" + _strColumnName + '\''
				+ ", _strOperator='" + _strOperator + '\'' + ", _objValue=" + _objValue + " " + _objValue.getClass()
				+ '}';
	}

}
