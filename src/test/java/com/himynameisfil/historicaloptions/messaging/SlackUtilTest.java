package com.himynameisfil.historicaloptions.messaging;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SlackUtilTest {
    private SlackUtil slackUtil;

    @Before
    public void setup() {
        slackUtil   =   new SlackUtil("src/test/resources/HistoricalOptionsIngestorSlack.properties");
    }


    @Test
    public void loadPropertiesHappy() throws IOException {
        Assert.assertEquals("testWebhookUrl", slackUtil.webhookUrl);
        Assert.assertEquals("testChannel", slackUtil.channel);
        Assert.assertEquals("testName", slackUtil.name);
    }

}