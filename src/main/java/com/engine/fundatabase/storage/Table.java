package com.engine.fundatabase.storage;

import java.util.Hashtable;
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
    private ArrayList<String> pagesId;
    private Hashtable<String, String> colNameType, colNameMin, colNameMax;
    private String primaryKeyType;
    private Hashtable<String, BTreeIndex<Row>> indexes;

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
        return Serializer.deserializePage(this.getName(), pageId);
    }
}
