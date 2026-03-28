package com.engine.fundatabase.database;

import java.util.Iterator;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;


public class SQLParser {

	
	Database database;

	public SQLParser(Database database) {
		this.database = database;
	}

	public Iterator<?> parse(StringBuffer input) {

		CharStream stream = CharStreams.fromString(input.toString());
		SQLiteLexer lexer = new SQLiteLexer(stream);
		CommonTokenStream token = new CommonTokenStream(lexer);
		SQLiteParser parser = new SQLiteParser(token);
		ParseTree tree = parser.parse();
		DatabaseListener databaseListener = new DatabaseListener(app);
		ParseTreeWalker.DEFAULT.walk(databaseListener, tree);
		return databaseListener.getResult();

	}

}
