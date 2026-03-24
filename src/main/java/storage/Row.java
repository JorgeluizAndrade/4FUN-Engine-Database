package storage;

import java.util.Vector;

import lombok.Getter;


@Getter
public class Row implements IRow {
	private Vector<Columns> columns;
	private Object primaryKey;
	
	
	public Row() {
		// TODO Auto-generated constructor stub
		columns = new Vector<>();
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

	
	public Vector<Columns> getColumns(){
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
		Row rCopy = null;
		
		try {
			rCopy = (Row) this.clone();
			return rCopy;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return rCopy;
	}
	
	@Override
	public String toString() {
		return "Tuple [columns =" + columns + ", primaryKey=" + primaryKey + "]";
	}
	
}
