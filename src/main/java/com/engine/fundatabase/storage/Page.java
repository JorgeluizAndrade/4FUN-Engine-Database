package com.engine.fundatabase.storage;

import java.util.Hashtable;
import java.util.Map;
import java.util.ArrayList;

import com.engine.fundatabase.utils.Constants;
import com.engine.fundatabase.utils.serializer.Serializer;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Page implements java.io.Serializable {
    private String name;
    private int maxRows;
    private ArrayList<Row> rows;
    private Object minPK, maxPK;
    private String tableName;

    public Page(String tableName) throws RuntimeException {
        this.rows = new ArrayList<>();
        this.tableName = tableName;
        maxRows = 1000;
    }

    public int getSize() {
        return rows.size();
    }

    protected Row removeLastRow(ArrayList<Row> rows) {
        Row res = rows.remove(rows.size() - 1);
        newMinMax();

        return res;

    }

    protected ArrayList<Row> select(Hashtable<String, Object> colNameValue, String operator) {
        return linearSearchWithOperator(this, operator, colNameValue);
    }

    protected void insertIntoPage(Row row) {
        int position = isEmpty() ? 0 : pageBinarySearch(row.getPrimaryKey());
        rows.add(position, row);
        newMinMax();
        Serializer.serializePage(name, this);
    }

    protected void deleteRowFromPage(Row row) {
        int position = isEmpty() ? 0 : pageBinarySearch(row.getPrimaryKey());
        rows.remove(position);
        newMinMax();
        Serializer.serializePage(name, this);
        handleEmptyPage();
    }

    protected void updateRowFromPage(Object clusteringKeyValue, Hashtable<String, Object> htblColNameValue) {
        int pkVectorPoition = pageBinarySearch(clusteringKeyValue);
        Row oldRow = rows.get(pkVectorPoition);

        for (Columns c : oldRow.getColumns()) {
            if (htblColNameValue.get(c.getKey()) != null)
                c.setValue(htblColNameValue.get(c.getKey()));
        }

        Serializer.serializePage(name, this);
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

    protected ArrayList<Row> linearSearch(Hashtable<String, Object> colNameValue) {
        return linearSearch(this, colNameValue);
    }

    public static ArrayList<Row> linearSearch(Page page, Hashtable<String, Object> colNameValue) {

        ArrayList<Row> results = new ArrayList<Row>();
        for (Row currRow : page.getRows()) {
            boolean isValid = true;
            for (Map.Entry<String, Object> curr : colNameValue.entrySet()) {
                String colName = curr.getKey();
                Object value = curr.getValue();
                Object currValue = getValueOfColInRow(currRow, colName);
                if (currValue == null || compare(currValue, value) != 0) {
                    isValid = false;
                    break;
                }
            }
            if (isValid)
                results.add(currRow);
        }
        return results;
    }

    private static ArrayList<Row> linearSearchWithOperator(Page page, String operator,
            Hashtable<String, Object> colNameValue) {
        ArrayList<Row> res = new ArrayList<>();
        String key = colNameValue.keys().nextElement();
        for (int i = 0; i < page.getSize(); i++) {
            Row row = page.getRows().get(i);
            Object rowVal = getValueOfColInRow(row, key);
            if (operatorBasedSelection(rowVal, colNameValue.get(key), operator))
                res.add(row);
        }
        return res;
    }

    private static boolean operatorBasedSelection(Object firstOperand, Object secondOperand, String operator) {
        if (operator.equals(Constants.EQUAL))
            return compare(firstOperand, secondOperand) == 0;
        if (operator.equals(Constants.GREATER_THAN))
            return compare(firstOperand, secondOperand) > 0;
        if (operator.equals(Constants.GREATER_THAN_OR_EQUAL))
            return compare(firstOperand, secondOperand) >= 0;
        if (operator.equals(Constants.LESS_THAN))
            return compare(firstOperand, secondOperand) < 0;
        if (operator.equals(Constants.LESS_THAN_OR_EQUAL))
            return compare(firstOperand, secondOperand) <= 0;
        else
            return compare(firstOperand, secondOperand) != 0;
    }

    private static int binarySearch(Page page, Object primaryKey) {

        int low = 0;
        int high = page.getSize() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            Row currRow = page.getRows().get(mid);
            Object pkValueOfCurrRow = currRow.getPrimaryKey();
            int comp = compare(primaryKey, pkValueOfCurrRow);
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
        return ((Comparable) first).compareTo(second);
    }

    private boolean isEmpty() {
        return this.rows.size() == 0;
    }

    private static Object getValueOfColInRow(Row currRow, String colName) {

        Object ret = null;
        for (Columns currColunmInRow : currRow.getColumns())
            if (currColunmInRow.getKey().equals(colName)) {
                ret = currColunmInRow.getValue();
                break;
            }
        return ret;
    }

    private void handleEmptyPage() {
        if (rows.isEmpty()) {
            System.out.println("Empty Page!");
        }
    }

}
