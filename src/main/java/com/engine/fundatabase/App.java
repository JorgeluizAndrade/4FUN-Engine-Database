package com.engine.fundatabase;

import java.util.Iterator;

import com.engine.fundatabase.database.Database;
import com.engine.fundatabase.storage.Row;

public class App {

    public static void main(String[] args) {
        Database database = new Database();
        database.executeSQL("CREATE TABLE heroes (id INT PRIMARY KEY, name TEXT);");
        System.out.println("CREATE TABLE ✓");

        database.executeSQL("INSERT INTO heroes (id, name) VALUES (1, 'Batman');");
        System.out.println("INSERT ✓");

        Iterator<?> result = database.executeSQL("SELECT id, name FROM heroes WHERE id = 1;");
        if (result != null && result.hasNext()) {
            Object firstResult = result.next();
            if (firstResult instanceof Row row) {
                System.out.println("SELECT id=1 \u2192 " + row.getValueColumn("name") + " ✓");
            } else {
                System.out.println(firstResult);
            }
        }

        System.out.println("Pipeline SQL -> ANTLR -> Listener -> Command -> Database executada.");
    }
}
