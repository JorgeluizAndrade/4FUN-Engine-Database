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
		// TODO Auto-generated method stub
		
	}


	@Override
	public void setPrimaryKey(Object pk) {
		// TODO Auto-generated method stub
		this.primaryKey = pk;
	}
		
	
}
