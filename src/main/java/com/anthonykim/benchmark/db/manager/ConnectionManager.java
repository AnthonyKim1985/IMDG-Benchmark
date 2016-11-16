package com.anthonykim.benchmark.db.manager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Properties;

public abstract class ConnectionManager {
    protected DBConnectionPoolManager connMgr;
    protected String poolName, dbServer, dbName, port, userID, password;
    protected int maxConn, initConn, maxWait;
    private Properties dbProperties;
    private String configFile;

    public ConnectionManager(String pool) {
        poolName = pool;
        configFile = pool;

        try {
            dbProperties = readProperties();
            dbServer = getProperty("dbServer");
            dbName = getProperty("dbName");
            port = getProperty("port");
            userID = getProperty("userID");
            password = getProperty("password");
            maxConn = Integer.parseInt(getProperty("maxConn"));
            initConn = Integer.parseInt(getProperty("initConn"));
            maxWait = Integer.parseInt(getProperty("maxWait"));
        } catch (IOException e) {
            Log.err("Error reading properties of " + configFile);
        }
    }

    public Connection getConnection() {
        return connMgr.getConnection(poolName);
    }

    public void freeConnection(Connection conn) {
        connMgr.freeConnection(poolName, conn);
    }

    private String getProperty(String prop) throws IOException {
        return (dbProperties.getProperty(prop));
    }

    protected synchronized Properties readProperties() throws IOException {
        Properties prop = new Properties();
        FileInputStream in = new FileInputStream(configFile);
        prop.load(in);
        return prop;
    }

    public int getDriverNumber() {
        return connMgr.getDriverNumber();
    }
}
