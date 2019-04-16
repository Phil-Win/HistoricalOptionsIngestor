package com.himynameisfil.historicaloptions.controller;

import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HistoricalOptionsIngestor {
    private String url;
    private String username;
    private String password;
    private Connection connect;
    private Statement statement;
    private ResultSet resultSet;
    private ResultSetMetaData resultMetaData;
    private String  inputFolderAsString;
    private List<String>    csvFilesToProcess;

    public HistoricalOptionsIngestor(String inputFolder) {
        loadProperties();
        this.connect = null;
        this.statement = null;
        this.resultSet = null;
    }

    public void loadInputFolder(String inputFolder) throws SQLException, ClassNotFoundException {
        this.inputFolderAsString    =   inputFolder ;
        String query;
        int columnsNumber;
        getAndSetAllCsvFileNames(new File(this.inputFolderAsString));
        Class.forName("com.mysql.jdbc.Driver");

        connect = DriverManager.getConnection(url, username, password);
        for (String fullCsvPath : csvFilesToProcess) {
            query    =   "LOAD DATA INFILE '" + fullCsvPath + "' INTO TABLE historical_options FIELDS TERMINATED BY ',' "
                    + " LINES TERMINATED BY '\n' IGNORE 1 ROWS";
            statement   =   connect.createStatement();
            resultSet   =   statement.executeQuery(query);
            while (resultSet.next()) {
                resultMetaData  =   resultSet.getMetaData();
                columnsNumber   =   resultMetaData.getColumnCount();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue + " " + resultMetaData.getColumnName(i));
                }
                System.out.println("");
            }
        }
    }

    private void getAndSetAllCsvFileNames(File folder) {
        List<String>    returnListOfCsvs    =   new ArrayList<String>();
        SlackUtil       slackUtil           =   new SlackUtil();
        for (File file : folder.listFiles()) {
            if (file.getName().contains(".csv")) {
                returnListOfCsvs.add(file.getAbsolutePath());
            } else {
                slackUtil.sendMessage("HistoricalOptionsIngestor: There is a non csv file in in the ingestion folder: " + file.getName());
            }
        }
        this.csvFilesToProcess  =   returnListOfCsvs;
    }


    private void loadProperties() {
        Resource resource   =   new FileSystemResource("/config/mysqloptions.properties");
        try {
            Properties props    = PropertiesLoaderUtils.loadProperties(resource);
            this.url =   props.getProperty("url");
            this.username    =   props.getProperty("username");
            this.password       =   props.getProperty("password");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
