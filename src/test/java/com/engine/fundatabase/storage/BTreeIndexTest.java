package com.engine.fundatabase.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

class BTreeIndexTest {

    @Test
    void shouldInsertAndSearchWithDuplicateKeys() {
        BTreeIndex<String> index = new BTreeIndex<>(2);

        index.insert(10, "a");
        index.insert(20, "b");
        index.insert(10, "c");

        List<String> values = index.search(10);
        assertEquals(2, values.size());
        assertEquals(List.of("a", "c"), values);
    }

    @Test
    void shouldSupportRangeSearches() {
        BTreeIndex<Integer> index = new BTreeIndex<>(3);
        for (int i = 1; i <= 30; i++) {
            index.insert(i, i);
        }

        assertEquals(10, index.searchGreaterThan(20, false).size());
        assertEquals(11, index.searchGreaterThan(20, true).size());
        assertEquals(9, index.searchLessThan(10, false).size());
        assertEquals(10, index.searchLessThan(10, true).size());
    }

    @Test
    void shouldReturnNotEqualResults() {
        BTreeIndex<Integer> index = new BTreeIndex<>(2);
        index.insert(1, 1);
        index.insert(2, 2);
        index.insert(2, 20);
        index.insert(3, 3);

        List<Integer> notEqual = index.searchNotEqual(2);
        assertEquals(List.of(1, 3), notEqual);
    }
}
