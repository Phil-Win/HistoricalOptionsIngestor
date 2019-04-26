package com.himynameisfil.historicaloptions.util;

import org.springframework.core.io.Resource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileUtil {
    private File fileOfInterest;
    private File destinationAsFile;


    public FileUtil(String fileOfInterest) {
        this.fileOfInterest =   new File(fileOfInterest);
    }
    public FileUtil(File file) {
        this.fileOfInterest =   file;
    }

    public Boolean isFile() {
        return fileOfInterest.isFile();
    }
    public Boolean isDirectory() {
        return fileOfInterest.isDirectory();
    }
    public Boolean exists() {
        return fileOfInterest.exists();
    }

    public Boolean moveFileTo(String destinationFolder) {
        destinationAsFile=   new File(destinationFolder);
        if (!isFile()) {
            return false;
        }
        try {
            if (destinationAsFile.isDirectory()) {
                Files.move(Paths.get(this.fileOfInterest.getAbsolutePath()),
                        Paths.get(destinationAsFile.getAbsolutePath() + File.separator + this.fileOfInterest.getName()), REPLACE_EXISTING);
                this.fileOfInterest =   new File(destinationFolder + File.separator + fileOfInterest.getName());
                return true;
            } else if (destinationAsFile.isFile()) {
                Files.move(Paths.get(this.fileOfInterest.getAbsolutePath()),
                        Paths.get(destinationAsFile.getAbsolutePath()), REPLACE_EXISTING);
                this.fileOfInterest =   new File(destinationAsFile.getAbsolutePath());
                return true;
            } else {
                System.out.println("I don't know how you got here but the destination file is neither a directory nor file: " + destinationFolder);
                return false;
            }
        } catch (Exception e){
            return false;
        }
    }

    public List<File> getCsvListInPathNonRecurse() {
        List<File>  returnList  =   new ArrayList<File>();
        if (isDirectory()) {
            for (File file : this.fileOfInterest.listFiles()) {
                if (file.getName().contains(".csv")) {
                    returnList.add(file);
                }
            }
        } else if (isFile()) {
            if (this.fileOfInterest.getName().contains(".csv")) {
                returnList.add(this.fileOfInterest);
            }
        }
        if (returnList.isEmpty()) {
            return null;
        } else {
            return returnList;
        }
    }

    public File getFileOfInterest() {
        return fileOfInterest;
    }
}
