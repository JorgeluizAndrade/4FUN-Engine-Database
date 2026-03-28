package com.engine.fundatabase.database.command;

import java.util.Iterator;

import com.engine.fundatabase.database.Database;

public interface SQLCommand {
    Iterator<?> execute(Database database);
}
