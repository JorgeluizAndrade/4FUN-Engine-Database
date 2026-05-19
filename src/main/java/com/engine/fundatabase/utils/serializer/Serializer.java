package com.engine.fundatabase.utils.serializer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.engine.fundatabase.storage.Page;
import com.engine.fundatabase.storage.Table;

public class Serializer {

    private final FileManager fileManager;

    public Serializer(String baseDir) {
        this.fileManager = new FileManager(baseDir);
    }

    public synchronized void serializeTable(Table tableObject) {
        String path = fileManager.getTablePath(tableObject.getName());

        System.out.println("PASSEI AQUI serializeTable: " + tableObject.getName());

        fileManager.ensureParentDirectoryExists(path);

        try (FileOutputStream fileOut = new FileOutputStream(path);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(tableObject);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing table", e);
        }
    }

    public synchronized Table deserializeTable(String tableName) {
        String path = fileManager.getTablePath(tableName);

        System.out.println("PASSEI AQUI deserializeTable");

        try (FileInputStream fileIn = new FileInputStream(path);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            System.out.println("path: " + path);
            return (Table) in.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing table", e);
        }
    }

    public synchronized void serializePage(String pageName, Page pageObject) {
        String path = fileManager.getPagePath(pageObject.getTableName(), pageName);

        fileManager.ensureParentDirectoryExists(path);

        try (FileOutputStream fileOut = new FileOutputStream(path);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(pageObject);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing page", e);
        }
    }

    public synchronized Page deserializePage(String tableName, String pageName) {
        String path = fileManager.getPagePath(tableName, pageName);

        try (FileInputStream fileIn = new FileInputStream(path);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            return (Page) in.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing page", e);
        }
    }
}
