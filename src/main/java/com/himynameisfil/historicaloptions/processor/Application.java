package com.himynameisfil.historicaloptions.processor;


import com.himynameisfil.historicaloptions.messaging.SlackUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;

@EnableScheduling
@SpringBootApplication
public class Application {
	private static final Logger log = LoggerFactory.getLogger(Application.class);
	public static void main(String[] args) {

		SlackUtil slackUtil   =   new SlackUtil();
		try {
			slackUtil.sendMessage("Application Started!");
			SpringApplication.run(Application.class, args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
