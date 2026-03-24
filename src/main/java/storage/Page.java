package storage;

import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

import lombok.Getter;
import lombok.Setter;

//Databases often use fixed-size <<<pages to store data>>>. 
//Tables, collections, rows, columns, indexes, sequences, 
// documents and more eventually end up as bytes in a page. 
// This way the storage engine can be 
//separated from the database frontend responsible for data format and API.


//<<<Databases read and write in pages>>>

@Getter
@Setter
public class Page {
	private String name;
	private int maxRows;
	private Vector<Row> rows;
	private Object minPK, maxPK;
	private String tableName;

	public Page(String tableName) throws RuntimeException {
		this.rows = new Vector<>();
		this.tableName = tableName;
		maxRows = 1000;
	}

	public int getSize() {
		return rows.size();
	}

	protected Row removeLastTuple(Vector<Row> rows) {
		Row res = rows.remove(rows.size() - 1);
		newMinMax();

		return res;

	}

	private void newMinMax() {
		if (rows.size() > 0) {
			minPK = rows.get(0).getPrimaryKey();
			maxPK = rows.get(rows.size() - 1).getPrimaryKey();
		}
	}

	public int pageBinarySearch(Object primaryKey) {
		return binarySearch(this, primaryKey);
	}

	protected Vector<Row> linearSearch(Hashtable<String, Object> colNameValue) {
		return linearSearch(this, colNameValue);
	}

	public static Vector<Row> linearSearch(Page page, Hashtable<String, Object> colNameValue) {

		Vector<Row> results = new Vector<Row>();
		for (Row currTuple : page.getRows()) {
			boolean isValid = true;
			for (Map.Entry<String, Object> curr : colNameValue.entrySet()) {
				String colName = curr.getKey();
				Object value = curr.getValue();
				Object currValue = getValueOfColInTuple(currTuple, colName);
				if (currValue == null || compare(currValue, value) != 0) {
					isValid = false;
					break;
				}
			}
			if (isValid)
				results.add(currTuple);
		}
		return results;
	}

	private static int binarySearch(Page page, Object primaryKey) {

		int low = 0;
		int high = page.getSize() - 1;
		while (low <= high) {
			int mid = low + (high - low) / 2;
			Row currTuple = page.getRows().get(mid);
			Object pkValueOfCurrTuple = currTuple.getPrimaryKey();
			int comp = compare(primaryKey, pkValueOfCurrTuple);
			if (comp == 0)
				return mid;
			else if (comp > 0)
				low = mid + 1;
			else
				high = mid - 1;
		}
		return low;
	}

	private static int compare(Object first, Object second) {
		return ((Comparable) first).compareTo((Comparable) second);
	}

	private static Object getValueOfColInTuple(Row currTuple, String colName) {

		Object ret = null;
		for (Columns currCellInTuple : currTuple.getColumns())
			if (currCellInTuple.getKey().equals(colName)) {
				ret = currCellInTuple.getValue();
				break;
			}
		return ret;
	}

}
