package com.himynameisfil.historicaloptions.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Properties;

public class SlackUtil {
    private static final Logger log = LoggerFactory.getLogger(SlackUtil.class);
    private     String      webhookUrl;
    private     String      channel;
    private     String      name;
    private final String WEBHOOK_VAR_NAME   =   "webhookUrl";
    private final String CHANNEL_VAR_NAME   =   "channel";
    private final String NAME_VAR_NAME      =   "name";

    public SlackUtil() {
        loadProperties();
    }

    public void sendMessage(String message) {
        SlackMessage messagePayload =   new SlackMessage(message);
        messagePayload.setChannel(channel);
        messagePayload.setUsername(name);
        RestTemplate restTemplate   =   new RestTemplate();
        restTemplate.postForObject(this.webhookUrl, messagePayload, String.class);
    }

    private void loadProperties() {
        Resource resource   =   new FileSystemResource("/config/HistoricalOptionsIngestorSlack.properties");
        try {
            Properties  props    = PropertiesLoaderUtils.loadProperties(resource);
            this.webhookUrl =   props.getProperty(WEBHOOK_VAR_NAME);
            this.channel    =   props.getProperty(CHANNEL_VAR_NAME);
            this.name       =   props.getProperty(NAME_VAR_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setName(String name) {
        this.name = name;
    }
}
