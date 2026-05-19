package com.engine.fundatabase.storage;

import java.util.Hashtable;
import java.util.Map.Entry;

import java.util.ArrayList;
import java.util.Locale;

import com.engine.fundatabase.utils.Constants;
import com.engine.fundatabase.utils.serializer.Serializer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Table implements java.io.Serializable {

	private static final long serialVersionUID = -4511440317382137672L;

	private String name, pkColumn;
	private Row row;
	private int size;
	private int cntPage;
	private ArrayList<String> pagesId = new ArrayList<>();
	private Hashtable<String, String> colNameType, colNameMin, colNameMax;
	private String primaryKeyType;
	private Hashtable<String, BTreeIndex<Row>> indexes = new Hashtable<String, BTreeIndex<Row>>();;

	private transient Serializer serializer;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {

		this.name = strTableName;
		this.pkColumn = strClusteringKeyColumn;
		this.colNameType = htblColNameType;
		this.colNameMin = htblColNameMin;
		this.colNameMax = htblColNameMax;
		size = 0;
		cntPage = 0;
		primaryKeyType = colNameType.get(pkColumn);
	}

	public Table(String name, String pkColumn, Row row, int size, int cntPage, ArrayList<String> pagesId,
			Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin,
			Hashtable<String, String> colNameMax, String primaryKeyType, Hashtable<String, BTreeIndex<Row>> indexes) {

		this.name = name;
		this.pkColumn = pkColumn;
		this.row = new Row();
		
		this.size = size;
		this.cntPage = cntPage;
		this.pagesId = pagesId != null ? pagesId : new ArrayList<>();

		this.colNameType = colNameType;
		this.colNameMin = colNameMin;
		this.colNameMax = colNameMax;

		this.primaryKeyType = primaryKeyType != null ? primaryKeyType
				: (colNameType != null ? colNameType.get(pkColumn) : null);

		this.indexes = indexes != null ? indexes : new Hashtable<>();

	}
	

	private void initSerializer() {
		this.serializer = new Serializer("data");
	}

	public void initializePhysicalStorage() {
		if (!isEmpty()) {
			return;
		}
		Page page = initializePage();
		page.persist();
	}

	public boolean isEmpty() {
		return pagesId == null || pagesId.isEmpty();
	}

	@Deprecated
	public boolean isEmpaty() {
		return isEmpty();
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

		System.out.println("PASSEI AQUI TABLE DSA");
		return result;
	}

	public void insertRow(Hashtable<String, Object> colunmNameValue) {
		Row row = createRow(colunmNameValue);

		if (isEmpty()) {
			insertNewPage(row);
		} else {
			int position = binarySearchPages(this, row.getPrimaryKey());
			insertRowAtPage(position, row);
		}

		System.out.println("PASSEI AQUI TABLE DSA INSERT ROW");

		size++;

	}

	private void insertNewPage(Row row) {
		Page page = initializePage();
		page.insertIntoPage(row, indexes);
		handleOverflow(page, cntPage - 1);
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
			validateColumnConstraints(String.valueOf(c.getKey()), c.getValue());
		}
		return row;
	}

	private void insertRowAtPage(int position, Row row) {
		Page page = getPageAtPosition(position);
		page.insertIntoPage(row, indexes);
		handleOverflow(page, position);
	}

	private void handleOverflow(Page page, int pagePosition) {
		Row overflowRow = page.splitOverflow(indexes);
		if (overflowRow == null) {
			return;
		}

		if (pagePosition + 1 >= pagesId.size()) {
			Page nextPage = initializePage();
			nextPage.insertIntoPage(overflowRow, indexes);
			handleOverflow(nextPage, pagesId.size() - 1);
			return;
		}

		insertRowAtPage(pagePosition + 1, overflowRow);
	}

	private void validateColumnConstraints(String columnName, Object value) {
		if (value == null || value instanceof DBAppNull) {
			return;
		}
		validateLowerBound(columnName, value);
		validateUpperBound(columnName, value);
	}

	private void validateLowerBound(String columnName, Object value) {
		String minValue = colNameMin == null ? null : colNameMin.get(columnName);
		if (minValue == null || minValue.isBlank()) {
			return;
		}
		Object parsedMin = parseConstraintValue(columnName, minValue);
		if (compare(value, parsedMin) < 0) {
			throw new IllegalArgumentException("Valor abaixo do mínimo permitido para " + columnName + ": " + value);
		}
	}

	private void validateUpperBound(String columnName, Object value) {
		String maxValue = colNameMax == null ? null : colNameMax.get(columnName);
		if (maxValue == null || maxValue.isBlank()) {
			return;
		}
		Object parsedMax = parseConstraintValue(columnName, maxValue);
		if (compare(value, parsedMax) > 0) {
			throw new IllegalArgumentException("Valor acima do máximo permitido para " + columnName + ": " + value);
		}
	}

	private Object parseConstraintValue(String columnName, String rawValue) {
		String normalizedType = colNameType.get(columnName);
		String text = rawValue.trim();
		if ((text.startsWith("'") && text.endsWith("'")) || (text.startsWith("\"") && text.endsWith("\""))) {
			text = text.substring(1, text.length() - 1);
		}
		if (normalizedType == null) {
			return text;
		}
		String typeName = normalizedType.toLowerCase(Locale.ROOT);
		if (typeName.contains("int")) {
			return Integer.parseInt(text);
		}
		if (typeName.contains("double") || typeName.contains("float") || typeName.contains("decimal")) {
			return Double.parseDouble(text);
		}
		if (typeName.contains("long")) {
			return Long.parseLong(text);
		}
		if (typeName.contains("boolean")) {
			return Boolean.parseBoolean(text);
		}
		return text;
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
		initSerializer();
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
			Page currPage = table.getPageAtPosition(mid);
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
		if (first == second) {
			return 0;
		}
		if (first == null || first instanceof DBAppNull) {
			return -1;
		}
		if (second == null || second instanceof DBAppNull) {
			return 1;
		}
		if (first instanceof Number && second instanceof Number) {
			Double firstDouble = ((Number) first).doubleValue();
			Double secondDouble = ((Number) second).doubleValue();
			return firstDouble.compareTo(secondDouble);
		}
		if (first instanceof Comparable) {
			return ((Comparable) first).compareTo(second);
		}
		throw new IllegalArgumentException("Valores não comparáveis: " + first + " e " + second);
    }

}
