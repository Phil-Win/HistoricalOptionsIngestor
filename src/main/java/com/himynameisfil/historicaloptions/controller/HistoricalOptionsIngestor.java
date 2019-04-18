package com.himynameisfil.historicaloptions.controller;

import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import com.himynameisfil.historicaloptions.model.HistoricalOption;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HistoricalOptionsIngestor {
    private static final String COMPLETED_FOLDER    =   "/data/historical_options_complete/";
    private String inputFolder;
    private String url;
    private String username;
    private String password;
    private List<String>    csvFilesToProcess;
    private List<String>    processedCsvFiles;
    private List<HistoricalOption>  listOfHistoricalOptionsRecords;
    private SlackUtil   slackUtil;

    public HistoricalOptionsIngestor(String inputFolder) {
        loadProperties();
        this.inputFolder    =   inputFolder;
        slackUtil   =   new SlackUtil();
        processedCsvFiles   =   new ArrayList<String>();

    }


    public void loadInputFolder() throws SQLException, ClassNotFoundException {
        listOfHistoricalOptionsRecords  =   new ArrayList<HistoricalOption>();

        //get all file names
        getAndSetAllCsvFileNames(new File(this.inputFolder));

        for (String file : csvFilesToProcess) {
            slackUtil.sendMessage(file);
        }
        if (this.csvFilesToProcess.size() != 0 ) {
            //get all records from all files
            for (String csvFile: csvFilesToProcess) {
                loadAllData(csvFile);
            }
            //load all records to mysql
            insertAllDataRecords();

            //move all processed files to completed

        }
    }

    private void loadAllData(String csvFile) {
        try {
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            CSVParser parser = new CSVParser(new FileReader(csvFile), format);

            for (CSVRecord record : parser) {
                HistoricalOption historicalOption = new HistoricalOption();
                historicalOption.setUnderlying(record.get("underlying"));
                historicalOption.setUnderlying_last(record.get("underlying_last"));
                historicalOption.setExchange(record.get(" exchange"));
                historicalOption.setOptionroot(record.get("optionroot"));
                historicalOption.setOptionext(record.get("optionext"));
                historicalOption.setType(record.get("type"));
                historicalOption.setExpiration(record.get("expiration"));
                historicalOption.setQuotedate(record.get("quotedate"));
                historicalOption.setStrike(record.get("strike"));
                historicalOption.setLast(record.get("last"));
                historicalOption.setBid(record.get("bid"));
                historicalOption.setAsk(record.get("ask"));
                historicalOption.setVolume(record.get("volume"));
                historicalOption.setOpeninterest(record.get("openinterest"));
                historicalOption.setImpliedvol(record.get("impliedvol"));
                historicalOption.setDelta(record.get("delta"));
                historicalOption.setGamma(record.get("gamma"));
                historicalOption.setTheta(record.get("theta"));
                historicalOption.setVega(record.get("vega"));
                historicalOption.setOptionalias(record.get("optionalias"));
                historicalOption.setIVBid(record.get("IVBid"));
                historicalOption.setIVAsk(record.get("IVAsk"));
                listOfHistoricalOptionsRecords.add(historicalOption);
            }
            this.processedCsvFiles.add(csvFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertAllDataRecords() throws ClassNotFoundException, SQLException {
        String query;
        PreparedStatement preparedStmt;
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection=  DriverManager.getConnection(this.url, this.username, this.password);
        try {
            query = "INSERT INTO historical_options " +
                    "(underlying, underlying_last, exchange, optionroot, optionext, " +
                    "type, expiration, quotedate, strike, last, " +
                    "bid, ask, volume, openinterest, impliedvol, " +
                    "delta, gamma, theta, vega, optionalias, " +
                    "IVBid, IVAsk) " +
                    " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            for (HistoricalOption historicalOption : this.listOfHistoricalOptionsRecords) {
                preparedStmt = connection.prepareStatement(query);
                preparedStmt.setString(1, historicalOption.getUnderlying());
                preparedStmt.setString(2, historicalOption.getUnderlying_last());
                preparedStmt.setString(3, historicalOption.getExchange());
                preparedStmt.setString(4, historicalOption.getOptionroot());
                preparedStmt.setString(5, historicalOption.getOptionext());
                preparedStmt.setString(6, historicalOption.getType());
                preparedStmt.setString(7, historicalOption.getExpiration());
                preparedStmt.setString(8, historicalOption.getQuotedate());
                preparedStmt.setString(9, historicalOption.getStrike());
                preparedStmt.setString(10, historicalOption.getLast());
                preparedStmt.setString(11, historicalOption.getBid());
                preparedStmt.setString(12, historicalOption.getAsk());
                preparedStmt.setString(13, historicalOption.getVolume());
                preparedStmt.setString(14, historicalOption.getOpeninterest());
                preparedStmt.setString(15, historicalOption.getImpliedvol());
                preparedStmt.setString(16, historicalOption.getDelta());
                preparedStmt.setString(17, historicalOption.getGamma());
                preparedStmt.setString(18, historicalOption.getTheta());
                preparedStmt.setString(19, historicalOption.getVega());
                preparedStmt.setString(20, historicalOption.getOptionalias());
                preparedStmt.setString(21, historicalOption.getIVBid());
                preparedStmt.setString(22, historicalOption.getIVAsk());
                preparedStmt.execute();
            }
        } catch (Exception e ) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void getAndSetAllCsvFileNames(File folder) {
        List<String>    returnListOfCsvs    =   new ArrayList<String>();
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

    private void moveToCompleted() throws IOException {
        //currently broken, idk why
        File    file;
        File    destinationFolder;
        String  fileName;
        for (String processCsv : this.processedCsvFiles) {
            file        =   new File(processCsv);
            destinationFolder    =   new File(COMPLETED_FOLDER);
            fileName    =   file.getName();
            if (file.isFile()) {
                slackUtil.sendMessage("valid file " + fileName);
            }
            if (destinationFolder.isDirectory()) {
                slackUtil.sendMessage("Completed folder is valid" + COMPLETED_FOLDER);
            }
            Path    temp    =   Files.move(Paths.get(processCsv), Paths.get(COMPLETED_FOLDER + fileName));
        }
    }

    public void setInputFolder(String inputFolder) {
        this.inputFolder = inputFolder;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setCsvFilesToProcess(List<String> csvFilesToProcess) {
        this.csvFilesToProcess = csvFilesToProcess;
    }

    public void setListOfHistoricalOptionsRecords(List<HistoricalOption> listOfHistoricalOptionsRecords) {
        this.listOfHistoricalOptionsRecords = listOfHistoricalOptionsRecords;
    }

    public void setSlackUtil(SlackUtil slackUtil) {
        this.slackUtil = slackUtil;
    }
}
