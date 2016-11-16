package com.anthonykim.benchmark.db.manager;

public class MySQLConnectionManager extends ConnectionManager {
    public MySQLConnectionManager(String configPath) {
        super(configPath);
        final String JDBCDriver = "com.mysql.jdbc.Driver";
        final String JDBCDriverType = "jdbc:mysql://";
        final String URL = JDBCDriverType + dbServer + ":" + port + "/" + dbName;
        connMgr = DBConnectionPoolManager.getInstance();
        connMgr.init(poolName, JDBCDriver, URL, userID, password, maxConn, initConn, maxWait);
    }
}