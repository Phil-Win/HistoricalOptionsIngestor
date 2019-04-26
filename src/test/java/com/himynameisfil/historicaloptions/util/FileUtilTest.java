package com.himynameisfil.historicaloptions.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.Matchers.any;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FileUtilTest {
    @Mock
    private File fileOfInterest;
    @Mock
    private File destinationAsFile;

    @InjectMocks
    FileUtil    fileUtil    =   new FileUtil("");

    @Test
    public void isFile_when_given_file() {
        when(fileOfInterest.isFile()).thenReturn(true);
        Assert.assertEquals(true, fileUtil.isFile());
    }

    @Test
    public void isFile_when_given_non_file() {
        when(fileOfInterest.isFile()).thenReturn(false);
        Assert.assertEquals(false, fileUtil.isFile());
    }

    @Test
    public void isDirectory_given_dir() {
        when(fileOfInterest.isDirectory()).thenReturn(true);
        Assert.assertEquals(true, fileUtil.isDirectory());
    }

    @Test
    public void isDirectory_given_non_dir() {
        when(fileOfInterest.isDirectory()).thenReturn(false);
        Assert.assertEquals(false, fileUtil.isDirectory());
    }

    @Test
    public void exists_given_existent_file() {
        when(fileOfInterest.exists()).thenReturn(true);
        Assert.assertEquals(true, fileUtil.exists());
    }

    @Test
    public void exists_given_non_existent_file() {
        when(fileOfInterest.exists()).thenReturn(false);
        Assert.assertEquals(false, fileUtil.exists());
    }

    @Test
    public void moveFileTo_source_is_file_dest_is_dir() throws IOException {
        //moves a file to the random folder and back
        FileUtil customFileUtil =   new FileUtil("src/test/resources/HistoricalOptionsIngestorSlack.properties");
        Assert.assertFalse(new File("src/test/resources/random/HistoricalOptionsIngestorSlack.properties").exists());
        customFileUtil.moveFileTo("src/test/resources/random");
        Assert.assertTrue(new File("src/test/resources/random/HistoricalOptionsIngestorSlack.properties").exists());
        customFileUtil.moveFileTo("src/test/resources");
        Assert.assertFalse(new File("src/test/resources/random/HistoricalOptionsIngestorSlack.properties").exists());
    }

    @Test
    public void getCsvListInPathNonRecurse() {
        when(fileUtil.isDirectory()).thenReturn(true);
        when(fileOfInterest.listFiles()).thenReturn(new File[]{new File("random/path/asdf.csv"), new File("another/notacsv.txt"), new File("agreat.csv")});
        List<File> expectedList =   new ArrayList<File>();
        expectedList.add(new File("random/path/asdf.csv"));
        expectedList.add(new File("agreat.csv"));

        Assert.assertEquals(expectedList, fileUtil.getCsvListInPathNonRecurse());
    }
}