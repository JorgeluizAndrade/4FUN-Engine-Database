package com.engine.fundatabase.storage;

import java.util.ArrayList;

import lombok.Getter;


@Getter
public class Row implements IRow, java.io.Serializable {
	private static final long serialVersionUID = -5475137508122924597L;
	private ArrayList<Columns> columns;
	private Object primaryKey;
	
	
	public Row() {
		// TODO Auto-generated constructor stub
		columns = new ArrayList<>();
	}

	@Override
	public void addColumn(Columns c) {
		this.columns.add(c);
	}
	
	
	@Override
	public void setPrimaryKey(Object pk) {
		// TODO Auto-generated method stub
		this.primaryKey = pk;
	}

	
	public ArrayList<Columns> getColumns(){
		return columns;
	}
	
	public Object getPrimaryKey() {
		return primaryKey;
	}
	
	
	public Object getValueColumn(String key) {
		Object ret = null;
		for (Columns column : columns) {
			if (column .getKey().equals(key))
				ret = column.getValue();
		}
		return ret;
	}
	
		
	protected Row getCopyRow() {
		Row copy = new Row();
		copy.setPrimaryKey(primaryKey);
		for (Columns column : columns) {
			// Columns é mutável, então a cópia precisa recriar cada entrada.
			copy.addColumn(new Columns(column.getKey(), column.getValue()));
		}
		return copy;
	}
	
	@Override
	public String toString() {
		return "Tuple [columns =" + columns + ", primaryKey=" + primaryKey + "]";
	}
	
}
