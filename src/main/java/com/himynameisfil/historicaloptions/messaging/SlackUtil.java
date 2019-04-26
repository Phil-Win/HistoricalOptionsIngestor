package com.himynameisfil.historicaloptions.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Properties;

public class SlackUtil {
    private static final Logger log = LoggerFactory.getLogger(SlackUtil.class);
    protected     String      webhookUrl;
    protected     String      channel;
    protected     String      name;
    protected     RestTemplate  restTemplate;
    protected     String        resourceFileName;
    private final String WEBHOOK_VAR_NAME   =   "webhookUrl";
    private final String CHANNEL_VAR_NAME   =   "channel";
    private final String NAME_VAR_NAME      =   "name";

    public SlackUtil(String resourceFileName) {
        setResourceFileName(resourceFileName);
        loadProperties();
    }
    public SlackUtil() {
        setResourceFileName("/config/HistoricalOptionsIngestorSlack.properties");
        loadProperties();
    }

    public Boolean sendMessage(String message) {
        SlackMessage messagePayload =   new SlackMessage(message);
        messagePayload.setChannel(channel);
        messagePayload.setUsername(name);
        restTemplate        =   new RestTemplate();
        ResponseEntity<String> response     =   restTemplate.postForEntity(this.webhookUrl, messagePayload, String.class);
        if (response.getStatusCode()    ==  HttpStatus.OK) {
            return true;
        } else {
            return false;
        }
    }

    protected void loadProperties() {
        try {
            Properties  props    = PropertiesLoaderUtils.loadProperties(new FileSystemResource(getResourceFileName()));
            this.webhookUrl =   props.getProperty(WEBHOOK_VAR_NAME);
            this.channel    =   props.getProperty(CHANNEL_VAR_NAME);
            this.name       =   props.getProperty(NAME_VAR_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Boolean   isValidWebhook() {
        return this.webhookUrl != null && !this.webhookUrl.isEmpty();
    }





    protected String getWebhookUrl() {
        return webhookUrl;
    }

    public void setWebhookUrl(String webhookUrl) {
        this.webhookUrl = webhookUrl;
    }

    protected String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    protected String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected RestTemplate getRestTemplate() {
        return restTemplate;
    }

    protected void setRestTemplate(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    protected String getResourceFileName() {
        return resourceFileName;
    }

    protected void setResourceFileName(String resourceFileName) {
        this.resourceFileName = resourceFileName;
    }
}
