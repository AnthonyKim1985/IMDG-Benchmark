package com.anthonykim.benchmark.dbcp.manager;

import org.apache.commons.dbcp.BasicDataSource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DataSource {
    private static DataSource dataSource;
    private BasicDataSource basicDataSource;

    private DataSource(Properties propsFile) throws IOException, SQLException, PropertyVetoException {
        basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        basicDataSource.setUsername(propsFile.getProperty("userID"));
        basicDataSource.setPassword(propsFile.getProperty("password"));
        basicDataSource.setUrl("jdbc:mysql://" + propsFile.getProperty("dbServer") + "/" + propsFile.getProperty("dbName"));

        basicDataSource.setMaxActive(Integer.parseInt(propsFile.getProperty("maxActive")));
        basicDataSource.setMinIdle(Integer.parseInt(propsFile.getProperty("minIdle")));
        basicDataSource.setMaxIdle(Integer.parseInt(propsFile.getProperty("maxIdle")));
        basicDataSource.setMaxOpenPreparedStatements(Integer.parseInt(propsFile.getProperty("maxOpenPreparedStatements")));

    }

    public static DataSource getInstance(Properties propsFile) throws IOException, SQLException, PropertyVetoException {
        if (dataSource == null) {
            dataSource = new DataSource(propsFile);
            return dataSource;
        } else {
            return dataSource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.basicDataSource.getConnection();
    }
}