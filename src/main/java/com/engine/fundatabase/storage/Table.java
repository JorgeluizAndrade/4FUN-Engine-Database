package com.engine.fundatabase.storage;

import java.util.Hashtable;
import java.util.Map.Entry;

import org.antlr.v4.codegen.model.chunk.ThisRulePropertyRef_ctx;

import java.util.ArrayList;

import com.engine.fundatabase.utils.Constants;
import com.engine.fundatabase.utils.serializer.Serializer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class Table implements java.io.Serializable {

	private String name, pkColumn;
	private Row row;
	private int size;
	private int cntPage;
	private ArrayList<String> pagesId;
	private Hashtable<String, String> colNameType, colNameMin, colNameMax;
	private String primaryKeyType;
	private Hashtable<String, BTreeIndex<Row>> indexes;
	
	private static Serializer serializer;

	
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {

	
		this.name = strClusteringKeyColumn;
		this.pkColumn = strClusteringKeyColumn;
		this.colNameType = htblColNameType;
		this.colNameMin = htblColNameMin;
		this.colNameMax = htblColNameMax;
	}

	
	
	public boolean isEmpaty() {
		return pagesId.size() == 0;
	}

	public void createIndex(String columnName, int minimumDegree) {
		if (indexes == null) {
			indexes = new Hashtable<>();
		}

		BTreeIndex<Row> index = new BTreeIndex<>(minimumDegree);
		for (int i = 0; i < pagesId.size(); i++) {
			Page page = getPageAtPosition(i);
			for (Row currentRow : page.getRows()) {
				Object value = currentRow.getValueColumn(columnName);
				if (value != null) {
					index.insert(value, currentRow);
				}
			}
		}
		indexes.put(columnName, index);
	}

	public ArrayList<Row> select(Hashtable<String, Object> colNameValue, String operator) {
		if (canUseIndex(colNameValue)) {
			String columnName = colNameValue.keys().nextElement();
			Object value = colNameValue.get(columnName);
			return searchUsingIndex(columnName, value, operator);
		}

		ArrayList<Row> result = new ArrayList<>();
		for (int i = 0; i < pagesId.size(); i++) {
			Page page = getPageAtPosition(i);
			result.addAll(page.select(colNameValue, operator));
		}
		return result;
	}

	public void insertRow(Hashtable<String, Object> colunmNameValue) {
		Row row = createRow(colunmNameValue);

		if (isEmpaty()) {
			insertNewPage(row);
		} else {
			int position = binarySearchPages(this, row.getPrimaryKey());
			Page page = getPageAtPosition(position);

			page.insertIntoPage(row, indexes);
		}

		size++;

	}

	private void insertNewPage(Row row) {
		Page page = initializePage();
		page.insertIntoPage(row, indexes);
	}

	private Page initializePage() {
		Page page = new Page(name);
		page.setName((cntPage++) + "");
		pagesId.add(page.getName());
		return page;
	}

	public Row createRow(Hashtable<String, Object> htblColNameValue) {
		Row row = new Row();
		for (Entry<String, String> entry : colNameType.entrySet()) {
			Columns c = new Columns(entry.getKey(), null);
			row.addColumn(c);
		}

		for (Columns c : row.getColumns()) {
			c.setValue(htblColNameValue.get(c.getKey()) == null ? new DBAppNull() : htblColNameValue.get(c.getKey()));
			if (c.getKey().equals(getPKColumn())) {
				row.setPrimaryKey(c.getValue());
			}
		}
		return row;
	}

	private boolean canUseIndex(Hashtable<String, Object> colNameValue) {
		if (indexes == null || colNameValue == null || colNameValue.size() != 1) {
			return false;
		}
		String columnName = colNameValue.keys().nextElement();
		return indexes.containsKey(columnName);
	}

	private ArrayList<Row> searchUsingIndex(String columnName, Object value, String operator) {
		BTreeIndex<Row> index = indexes.get(columnName);
		ArrayList<Row> rows = new ArrayList<>();

		if (Constants.EQUAL.equals(operator)) {
			rows.addAll(index.search(value));
		} else if (Constants.GREATER_THAN.equals(operator)) {
			rows.addAll(index.searchGreaterThan(value, false));
		} else if (Constants.GREATER_THAN_OR_EQUAL.equals(operator)) {
			rows.addAll(index.searchGreaterThan(value, true));
		} else if (Constants.LESS_THAN.equals(operator)) {
			rows.addAll(index.searchLessThan(value, false));
		} else if (Constants.LESS_THAN_OR_EQUAL.equals(operator)) {
			rows.addAll(index.searchLessThan(value, true));
		} else {
			rows.addAll(index.searchNotEqual(value));
		}
		return rows;
	}

	public Page getPageAtPosition(int position) {
		String pageId = pagesId.get(position);
		return serializer.deserializePage(this.getName(), pageId);
	}

	public String getPKColumn() {
		return pkColumn;
	}

	private static int binarySearchPages(Table table, Object tuplePrimaryKey) {
		int n = table.getPagesId().size();
		int low = 0;
		int high = n - 1;

		while (low <= high) {
			int mid = (low + high) / 2;
			Page currPage = serializer.deserializePage(table.getName(), table.getPagesId().get(mid));
			int compareWithMin = compare(tuplePrimaryKey, currPage.getMinPK());
			int compWithMax = compare(tuplePrimaryKey, currPage.getMaxPK());
			if (compWithMax <= 0 && compareWithMin >= 0) {
				return mid;
			} else if (compWithMax > 0)
				low = mid + 1;
			else
				high = mid - 1;
		}

		return Math.min(low, n - 1);
	}

	private static int compare(Object first, Object second) {
		return ((Comparable) first).compareTo(second);
	}

	
}
