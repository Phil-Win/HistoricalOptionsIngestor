package com.himynameisfil.historicaloptions.processor;

import com.himynameisfil.historicaloptions.controller.HistoricalOptionsIngestor;
import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Component
public class Scheduler {
    private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

    @Scheduled(cron = "0 * * * * *")
    public void processHistoricalOptionsData() {
        SlackUtil slackUtil   =   new SlackUtil();
        slackUtil.sendMessage("Started Daily Historical Options Ingestor : " + new SimpleDateFormat("MM-dd-yyyy hh:mm").format(new Date()));
        HistoricalOptionsIngestor historicalOptionsIngestor =   new HistoricalOptionsIngestor();

        slackUtil.sendMessage("Finished Daily Historical Options Ingestor : " + new SimpleDateFormat("MM-dd-yyyy hh:mm").format(new Date()));
    }

}
