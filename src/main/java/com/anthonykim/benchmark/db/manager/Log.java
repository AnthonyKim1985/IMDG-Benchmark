package com.anthonykim.benchmark.db.manager;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class Log {
    public final String logFile = "connection-pool.log";
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private FileWriter fileWriter;

    public Log() {
        try {
            fileWriter = new FileWriter(logFile, true);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public void close(FileWriter fileWriter) {
        try {
            fileWriter.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public void debug(String message) {
        try {
            fileWriter.write(new Date() + " : ");
            fileWriter.write(message + LINE_SEPARATOR);
            fileWriter.flush();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void out(String message) {
        System.out.println(new Date() + ": " + message);
    }

    public static void err(String message) {
        System.out.println(new Date() + ": " + message);
    }

    public static void err(Throwable e, String message) {
        System.err.println(new Date() + ": " + message);
        e.printStackTrace(System.out);
    }
}