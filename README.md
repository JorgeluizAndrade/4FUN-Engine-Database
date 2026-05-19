Make my own SQL engine, called 4fun databate; This is an exercise in building a minimal in-memory SQL-like database for educational purposes and to fun with code.
Almost all of the implementation ideas come from these resources:
- [Fabio Akita](https://youtu.be/_7nISfpofec?si=pW1GsOC7mzQHyMVL)
- [Tony Saro](https://youtu.be/5Pc18ge9ohI?si=1r998J88q2AmbJ0L)

## FunEngine

FunEngine is a small educational database engine written in Java. The project parses a subset of SQL, converts it into commands, executes those commands against an internal storage layer, and persists tables/pages on disk using Java serialization.

The current implementation is focused on learning and experimentation, not production use.

## What The Project Does

- Accepts simple SQL commands such as `CREATE TABLE`, `INSERT`, and `SELECT`
- Parses SQL with ANTLR
- Executes commands through a database service layer
- Stores table metadata and pages under the local `data/` directory
- Keeps table pages sorted by primary key
- Supports basic page splitting when a page reaches capacity
- Supports in-memory table caching to reduce repeated deserialization
- Includes a simple socket-based server/client flow

## Main Components

- `src/main/java/com/engine/fundatabase/App.java`
  Demo entrypoint that runs a basic `CREATE`, `INSERT`, and `SELECT`
- `src/main/java/com/engine/fundatabase/database/`
  Core database API and SQL command execution flow
- `src/main/java/com/engine/fundatabase/storage/`
  Table, page, row, and index structures
- `src/main/java/com/engine/fundatabase/parser/`
  SQL parsing pipeline
- `src/main/java/com/engine/fundatabase/network/`
  Simple TCP server/client for sending SQL over a socket
- `src/main/java/com/engine/fundatabase/utils/serializer/`
  Persistence helpers for tables and pages

## Tech Stack

- Java 21
- Maven
- JUnit 5
- ANTLR 4
- Lombok

## Project Structure

```text
funengine/
├── data/                        # Serialized tables and pages
├── src/main/java/com/engine/fundatabase/
│   ├── App.java
│   ├── database/
│   ├── network/
│   ├── parser/
│   ├── storage/
│   └── utils/
├── src/test/java/com/engine/fundatabase/
├── pom.xml
└── README.md
```

## Requirements

Before running the project, make sure you have:

- JDK 21 installed
- Maven installed and available in `PATH`

Check versions with:

```bash
java -version
mvn -version
```

## How To Run

### 1. Compile the project

```bash
mvn compile
```

### 2. Run the demo application

This runs the sample flow in `App.java`.

```bash
mvn -q -DskipTests compile org.codehaus.mojo:exec-maven-plugin:3.1.0:java -Dexec.mainClass=com.engine.fundatabase.App -Dexec.classpathScope=runtime
```

Expected output includes:

```text
CREATE TABLE ✓
INSERT ✓
SELECT id=1 → Batman ✓
```

### 3. Run the tests

```bash
mvn test
```

## Running The SQL Server

The project also has a simple socket server on port `8080`.

### Start the server

```bash
mvn -Pserver exec:java
```

### Start the client

In another terminal:

```bash
mvn -Pclient exec:java
```

Then type a SQL command such as:

```sql
SELECT id, name FROM heroes WHERE id = 1;
```

## Example SQL

```sql
CREATE TABLE heroes (id INT PRIMARY KEY, name TEXT);
INSERT INTO heroes (id, name) VALUES (1, 'Batman');
SELECT id, name FROM heroes WHERE id = 1;
```

## How It Works Internally

1. SQL text is received by `App`, `SQLClient`, or another caller.
2. `SQLParser` uses ANTLR to build a parse tree.
3. `DatabaseListener` transforms the parse result into command objects.
4. `Database` executes those commands.
5. `Table` and `Page` manage row insertion, ordering, and page overflow.
6. `Serializer` persists table and page state under `data/`.

## Notes And Current Limitations

- This is an educational engine and still implements only a small SQL subset.
- Persistence is based on Java serialization, which is simple but limited.
- The data files in `data/` are local artifacts and may change between runs.
- The parser currently targets straightforward `WHERE` expressions and simple command flows.
- Logging is intentionally simple and currently uses `System.out.println`.

## Useful Commands

```bash
mvn compile
mvn test
mvn -Pserver exec:java
mvn -Pclient exec:java
```
