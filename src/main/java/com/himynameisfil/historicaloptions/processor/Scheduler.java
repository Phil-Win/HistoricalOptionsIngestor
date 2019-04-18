package com.himynameisfil.historicaloptions.processor;

import com.himynameisfil.historicaloptions.controller.HistoricalOptionsIngestor;
import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    @Scheduled(cron = "0 0 11 * * *")
    public void processHistoricalOptionsData() {
        SlackUtil slackUtil   =   new SlackUtil();
        slackUtil.sendMessage("Started Daily Historical Options Ingestor : " + new SimpleDateFormat("MM-dd-yyyy hh:mm").format(new Date()));
        HistoricalOptionsIngestor historicalOptionsIngestor =   new HistoricalOptionsIngestor("/data/historical_options_input");

        try {
            historicalOptionsIngestor.loadInputFolder();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        /*
        try {
            Class.forName("com.mysql.jdbc.Driver");

            Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/options", "root", "acleverpass");
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("desc historical_options");
            ResultSetMetaData rsmd = rs.getMetaData();
            System.out.println("desc historical_options");
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue + " " + rsmd.getColumnName(i));
                }
                System.out.println("");
            }
            connection.close();
        } catch (Exception e) {

        }
        */
        slackUtil.sendMessage("Finished Daily Historical Options Ingestor : " + new SimpleDateFormat("MM-dd-yyyy hh:mm").format(new Date()));
    }

}
