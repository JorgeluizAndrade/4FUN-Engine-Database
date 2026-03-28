package com.engine.fundatabase;

import com.engine.fundatabase.database.Database;

public class App {

    public static void main(String[] args) {
        Database database = new Database();
        database.executeSQL("CREATE TABLE heroes (id INT PRIMARY KEY, name TEXT);");
        database.executeSQL("INSERT INTO heroes (id, name) VALUES (1, 'Batman');");
        database.executeSQL("SELECT id, name FROM heroes WHERE id = 1;");

        System.out.println("Pipeline SQL -> ANTLR -> Listener -> Command -> Database executada.");
    }
}
