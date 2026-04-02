package com.engine.fundatabase.database;

import java.util.Iterator;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import funengine.sql.SQLiteLexer;
import funengine.sql.SQLiteParser;

public class SQLParser {

    private final Database database;

    public SQLParser(Database database) {
        this.database = database;
    }

    public Iterator<?> parse(String input) {

        CharStream stream = CharStreams.fromString(input);
        SQLiteLexer lexer = new SQLiteLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SQLiteParser
        parser = new SQLiteParser(tokenStream);

        ParseTree tree = parser.parse();
        DatabaseListener databaseListener = new DatabaseListener(database);
        ParseTreeWalker.DEFAULT.walk(databaseListener, tree);
        return databaseListener.executeCommands();
    }
}