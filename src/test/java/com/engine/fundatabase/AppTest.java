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

    @Test
    void shouldSplitPageWhenCapacityIsExceeded() {
        Table table = createTable("overflowTable", new Hashtable<>(), new Hashtable<>());

        for (int id = 1; id <= 6; id++) {
            table.insertRow(createRowData(id));
        }

        assertEquals(2, table.getPagesId().size());
        assertEquals(5, table.getPageAtPosition(0).getRows().size());
        assertEquals(1, table.getPageAtPosition(1).getRows().size());
        assertEquals(6, table.getPageAtPosition(1).getRows().get(0).getPrimaryKey());
    }

    @Test
    void shouldValidateMinAndMaxConstraintsBeforeInsert() {
        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "10");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "20");

        Table table = createTable("constraintsTable", minValues, maxValues);

        IllegalArgumentException belowMin = assertThrows(IllegalArgumentException.class,
                () -> table.insertRow(createRowData(9)));
        assertTrue(belowMin.getMessage().contains("mínimo"));

        IllegalArgumentException aboveMax = assertThrows(IllegalArgumentException.class,
                () -> table.insertRow(createRowData(21)));
        assertTrue(aboveMax.getMessage().contains("máximo"));
    }

    @Test
    void shouldExposeIsEmptyWithoutBreakingLegacyTypoMethod() {
        Table table = createTable();

        assertTrue(table.isEmpty());
        assertTrue(table.isEmpaty());

        table.insertRow(createRowData(1));

        assertFalse(table.isEmpty());
        assertFalse(table.isEmpaty());
    }

    private Table createTable() {
        return createTable("testTable", new Hashtable<>(), new Hashtable<>());
    }

    private Table createTable(String tableName, Hashtable<String, String> minValues, Hashtable<String, String> maxValues) {
        Hashtable<String, String> colTypes = new Hashtable<>();
        colTypes.put("id", "java.lang.Integer");

        return new Table(
            tableName,
            "id",
            new Row(),
            0,
            0,
            new ArrayList<>(),
            colTypes,
            minValues,
            maxValues,
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
