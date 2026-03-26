package com.engine.fundatabase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Hashtable;

import org.junit.jupiter.api.Test;

import com.engine.fundatabase.utils.Constants;

class TableIndexSelectTest {

    @Test
    void shouldUseBTreeIndexForSelectOperators() {
        Hashtable<String, BTreeIndex<Row>> indexes = new Hashtable<>();
        BTreeIndex<Row> ageIndex = new BTreeIndex<>(2);

        Row row1 = row(1, "age", 18);
        Row row2 = row(2, "age", 25);
        Row row3 = row(3, "age", 30);

        ageIndex.insert(18, row1);
        ageIndex.insert(25, row2);
        ageIndex.insert(30, row3);

        indexes.put("age", ageIndex);

        Table table = new Table(
                "heroes",
                "id",
                null,
                3,
                new ArrayList<>(),
                new Hashtable<>(),
                new Hashtable<>(),
                new Hashtable<>(),
                "java.lang.Integer",
                indexes);

        assertEquals(1, table.select(query("age", 25), Constants.EQUAL).size());
        assertEquals(2, table.select(query("age", 20), Constants.GREATER_THAN).size());
        assertEquals(2, table.select(query("age", 25), Constants.GREATER_THAN_OR_EQUAL).size());
        assertEquals(1, table.select(query("age", 20), Constants.LESS_THAN).size());
        assertEquals(2, table.select(query("age", 25), Constants.LESS_THAN_OR_EQUAL).size());
        assertEquals(2, table.select(query("age", 25), Constants.NOT_EQUAL).size());
    }

    private static Hashtable<String, Object> query(String key, Object value) {
        Hashtable<String, Object> query = new Hashtable<>();
        query.put(key, value);
        return query;
    }

    private static Row row(int id, String key, Object value) {
        Row row = new Row();
        row.setPrimaryKey(id);
        row.addColumn(new Columns("id", id));
        row.addColumn(new Columns(key, value));
        return row;
    }
}
