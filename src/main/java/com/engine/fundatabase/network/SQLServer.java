package com.engine.fundatabase.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.engine.fundatabase.database.Database;

public class SQLServer {

	private static final String ART = 
			"'       __    _____   __    _______________ __________     .____            ____.  _______    ________  .____  .__   ____.  _______      __          __   \n"
			+ "'      / /   /  |  |  \\ \\   \\_   _____/    |   \\      \\    |   _|   ____   |_   |  \\      \\  /  _____/  |   _| |__| |_   |  \\      \\    / /   ____   \\ \\  \n"
			+ "'     / /   /   |  |_  \\ \\   |    __) |    |   /   |   \\   |  |   _/ __ \\    |  |  /   |   \\/   \\  ___  |  |   |  |   |  |  /   |   \\  / /  _/ __ \\   \\ \\ \n"
			+ "'     \\ \\  /    ^   /  / /   |     \\  |    |  /    |    \\  |  |   \\  ___/    |  | /    |    \\    \\_\\  \\ |  |   |  |   |  | /    |    \\ \\ \\  \\  ___/   / / \n"
			+ "'      \\_\\ \\____   |  /_/    \\___  /  |______/\\____|__  /  |  |_   \\___  >  _|  | \\____|__  /\\______  / |  |_  |__|  _|  | \\____|__  /  \\_\\  \\___  > /_/  \n"
			+ "'               |__|             \\/                   \\/   |____|      \\/  |____|         \\/        \\/  |____|      |____|         \\/            \\/       "
			;
	
	private static final String ABOUT = "This projects was created by Jorge Luiz Andrade de Souza at Brasil. "
			+ "\nThe main goal of this project is to fun and learn about databases, SQL and network programming. "
			+ "\nFeel free to explore the code, contribute with new features or just have fun playing with it!"
			+ "Jesus loves you and I hope this project brings to you as much joy as it brought to me while creating it!";
	

	
    private final int port;
    private final Database database;

    
    
    public SQLServer(int port, Database database) {
        this.port = port;
        this.database = database;
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
        	System.out.println(ART);
        	
        	System.out.println("\n" + "\n" + ABOUT);
        	
            System.out.println("\nSQL server running on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                handleClient(clientSocket);
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (Socket socket = clientSocket;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            String query = reader.readLine();
            if (query == null || query.isBlank()) {
                writer.println("ERROR: empty query");
                return;
            }

            Iterator<?> result = database.executeSQL(query);
            if (result == null) {
                writer.println("OK");
                return;
            }

            List<String> rows = new ArrayList<>();
            while (result.hasNext()) {
                Object row = result.next();
                rows.add(String.valueOf(row));
            }

            writer.println(rows.isEmpty() ? "EMPTY" : String.join(" | ", rows));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Database database = new Database();
        new SQLServer(8080, database).start();
    }
}
