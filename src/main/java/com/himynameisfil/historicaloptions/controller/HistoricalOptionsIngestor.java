package com.himynameisfil.historicaloptions.controller;

import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import com.himynameisfil.historicaloptions.model.HistoricalOption;
import com.himynameisfil.historicaloptions.util.FileUtil;
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
    private static final String COMPLETED_FOLDER    =   "/data/historical_options_complete";
    private String inputFolder;
    private String url;
    private String username;
    private String password;
    private List<File>    csvFilesToProcess;
    private List<File>    processedCsvFiles;
    private List<HistoricalOption>  listOfHistoricalOptionsRecords;
    private SlackUtil   slackUtil;
    private FileUtil    fileUtil;

    public HistoricalOptionsIngestor(String inputFolder) {
        loadProperties();
        this.inputFolder    =   inputFolder;
        slackUtil   =   new SlackUtil("/config/HistoricalOptionsIngestorSlack.properties");
        processedCsvFiles   =   new ArrayList<File>();

    }


    public void loadInputFolder() throws SQLException, ClassNotFoundException, IOException {
        listOfHistoricalOptionsRecords  =   new ArrayList<HistoricalOption>();

        //get all file names
        this.csvFilesToProcess  =   get2CsvFileNames(this.inputFolder);
/*
        for (File file : csvFilesToProcess) {
            slackUtil.sendMessage(file.getAbsolutePath());
        }
*/
        if (this.csvFilesToProcess.size() != 0 ) {
            //get all records from all files
            for (File csvFile: csvFilesToProcess) {
                loadAllData(csvFile);
            }
            //load all records to mysql
            insertAllDataRecords();

            //move all processed files to completed
            moveToCompleted();

        }
    }

    private void loadAllData(File csvFile) {
        try {
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            CSVParser parser = new CSVParser(new FileReader(csvFile.getAbsolutePath()), format);

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

    public List<File> get2CsvFileNames(String folder) {
        fileUtil    =   new FileUtil(folder);
        List<File>    returnListOfCsvs    =   fileUtil.getCsvListInPathNonRecurse();

        if ( returnListOfCsvs.size() >= 2 ) {
            return returnListOfCsvs.subList(0,2);
        } else {
            return returnListOfCsvs;
        }
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
        FileUtil    fileUtil;
        for (File processCsv : this.processedCsvFiles) {
            fileUtil    =   new FileUtil(processCsv);
            if (fileUtil.moveFileTo(COMPLETED_FOLDER)) {
                slackUtil.sendMessage("Sent the file to completed: " + processCsv.getAbsolutePath() + " was sent to " + fileUtil.getFileOfInterest().getAbsolutePath());
            } else {
                slackUtil.sendMessage("Failed to send file to completed: " + processCsv.getAbsolutePath());
            }
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


    public void setListOfHistoricalOptionsRecords(List<HistoricalOption> listOfHistoricalOptionsRecords) {
        this.listOfHistoricalOptionsRecords = listOfHistoricalOptionsRecords;
    }

    public void setSlackUtil(SlackUtil slackUtil) {
        this.slackUtil = slackUtil;
    }

    public List<File> getProcessedCsvFiles() {
        return processedCsvFiles;
    }

}
