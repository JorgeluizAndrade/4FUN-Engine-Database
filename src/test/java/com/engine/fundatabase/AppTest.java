package com.engine.fundatabase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.engine.fundatabase.storage.Columns;
import com.engine.fundatabase.storage.Page;
import com.engine.fundatabase.storage.Row;
import com.engine.fundatabase.storage.Table;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Hashtable;

class AppTest {

    @Test
    void shouldRun() {
        assertTrue(true);
    }
    
    
    @BeforeEach
    void setup() {
        new java.io.File("testTable").mkdirs();
    }

    
    
    @Test
    void shouldInsertRowsInSortedOrder() {
        Page page = new Page("testTable");

        Row r1 = createRow(10);
        Row r2 = createRow(5);
        Row r3 = createRow(20);

        page.insertIntoPage(r1, null);
        page.insertIntoPage(r2, null);
        page.insertIntoPage(r3, null);

        assertEquals(3, page.getRows().size());

        // verify order
        assertEquals(5, page.getRows().get(0).getPrimaryKey());
        assertEquals(10, page.getRows().get(1).getPrimaryKey());
        assertEquals(20, page.getRows().get(2).getPrimaryKey());

        // verify min and max
        assertEquals(5, page.getMinPK());
        assertEquals(20, page.getMaxPK());
    }

    private Row createRow(int pk) {
        Row row = new Row();
        Columns column = new Columns("id", pk);
        row.addColumn(column);
        row.setPrimaryKey(pk);
        return row;
    }
    
    
    @Test
    void shouldCreateFirstPageWhenTableIsEmpty() {
        Table table = createTable();

        Hashtable<String, Object> data = new Hashtable<>();
        data.put("id", 10);

        table.insertRow(data);

        assertEquals(1, table.getPagesId().size());
        assertEquals(1, table.getSize());
    }

    @Test
    void shouldInsertRowsInSortedOrderInsidePage() {
        Table table = createTable();

        table.insertRow(createRowData(10));
        table.insertRow(createRowData(5));
        table.insertRow(createRowData(20));

        Page page = table.getPageAtPosition(0);

        assertEquals(5, page.getRows().get(0).getPrimaryKey());
        assertEquals(10, page.getRows().get(1).getPrimaryKey());
        assertEquals(20, page.getRows().get(2).getPrimaryKey());
    }

    private Table createTable() {
        Hashtable<String, String> colTypes = new Hashtable<>();
        colTypes.put("id", "java.lang.Integer");

        return new Table(
            "testTable",
            "id",
            new Row(),
            0,
            0,
            new ArrayList<>(),
            colTypes,
            new Hashtable<>(),
            new Hashtable<>(),
            "java.lang.Integer",
            null
        );
    }

    private Hashtable<String, Object> createRowData(int id) {
        Hashtable<String, Object> data = new Hashtable<>();
        data.put("id", id);
        return data;
    }


    
}
