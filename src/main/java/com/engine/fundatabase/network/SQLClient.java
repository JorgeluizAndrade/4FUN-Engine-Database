package com.engine.fundatabase.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class SQLClient {

    public static void main(String[] args) throws IOException {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.print("SQL> ");
            String query = scanner.nextLine();

            try (Socket socket = new Socket("127.0.0.1", 8080);
                 PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                writer.println(query);
                String response = reader.readLine();
                System.out.println("Server response: " + response);
            }
        }
    }
}
