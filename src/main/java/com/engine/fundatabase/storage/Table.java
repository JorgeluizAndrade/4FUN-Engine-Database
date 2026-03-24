package com.engine.fundatabase.storage;

import java.util.Hashtable;
import java.util.Vector;

import com.engine.fundatabase.utils.serializer.Serializer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@AllArgsConstructor
@Getter
@Setter
public class Table {

	private String name, pkColumn;
	private Row row;
	private int size;
	private Vector<String> pagesId;
	private Hashtable<String, String> colNameType, colNameMin, colNameMax;
	private String primaryKeyType;
	
	
	public boolean isEmpaty() {
		return pagesId.size() == 0;
	}
	
	public Vector<Row> select(Hashtable<String, Object> colNameValue, String operator){
		Vector<Row> result = new Vector<>();
		for (int i = 0; i < pagesId.size(); i++) {
			Page page = getPageAtPosition(i);
			result.addAll(page.select(colNameValue, operator));
		}
		return result;
	}
	

	public Page getPageAtPosition(int position) {
		String pageId = pagesId.get(position);
		Page page = Serializer.deserializePage(this.getName(), pageId);
		return page;
	}
}
