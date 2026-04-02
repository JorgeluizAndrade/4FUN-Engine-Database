package com.engine.fundatabase.utils.serializer;

import java.io.*;

import com.engine.fundatabase.storage.Page;
import com.engine.fundatabase.storage.Table;
import com.engine.fundatabase.utils.Constants;

public class Serializer {

    private final String baseDir;

    public Serializer(String baseDir) {
        this.baseDir = (baseDir == null || baseDir.isBlank()) ? "data" : baseDir;
    }

    public void serializeTable(Table tableObject) {
        String path = getPath(tableObject.getName(), tableObject.getName());
        
        System.out.println("PASSEI AQUI serializeTable: " + tableObject.getName());


        File file = new File(path);
        File parent = file.getParentFile();

        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (FileOutputStream fileOut = new FileOutputStream(file);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {

            out.writeObject(tableObject);

        } catch (IOException e) {
            throw new RuntimeException("Error serializing table", e);
        }
    }

    public Table deserializeTable(String tableName) {
        String path = getPath(tableName, tableName);
        
        
        System.out.println("PASSEI AQUI deserializeTable");


        try (FileInputStream fileIn = new FileInputStream(path);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {

        	
        	System.out.println("path: " + path);
        	
            return (Table) in.readObject();

        } catch (Exception e) {
            throw new RuntimeException("Error deserializing table", e);
        }
    }

    public void serializePage(String pageName, Page pageObject) {
        String path = getPath(pageObject.getTableName(), pageName);

        File file = new File(path);
        File parent = file.getParentFile();

        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        try (FileOutputStream fileOut = new FileOutputStream(file);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {

            out.writeObject(pageObject);

        } catch (IOException e) {
            throw new RuntimeException("Error serializing page", e);
        }
    }

    public Page deserializePage(String tableName, String pageName) {
        String path = getPath(tableName, pageName);

        try (FileInputStream fileIn = new FileInputStream(path);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {

            return (Page) in.readObject();

        } catch (Exception e) {
            throw new RuntimeException("Error deserializing page", e);
        }
    }

    private String getPath(String tableName, String targetName) {
        return baseDir + File.separator +
               tableName + File.separator +
               targetName + Constants.DATA_EXTENSTION;
    }
}
