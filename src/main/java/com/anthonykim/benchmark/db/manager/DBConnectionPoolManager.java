package com.anthonykim.benchmark.db.manager;

import java.sql.Connection;
import java.util.Hashtable;
import java.util.Vector;

public class DBConnectionPoolManager {
    // DBConnectionPoolManager에 싱글턴 패턴을 적용하기 위해 static으로 선언(인스턴스 1개 유지)
    private static DBConnectionPoolManager instance;
    private Vector<String> drivers = new Vector<String>();
    private Hashtable<String, DBConnectionPool> pools = new Hashtable<String, DBConnectionPool>();

    // DBConnectionPoolManager의 instance를 얻음
    // @return DBConnectionManager
    public static synchronized DBConnectionPoolManager getInstance() {
        if (instance == null)
            instance = new DBConnectionPoolManager();
        return instance;
    }

    // Default Constructor
    private DBConnectionPoolManager() {
    }

    // 현재 Connection을 Free Connection List로 보냄
    // @param name : Pool Name
    // @param con : Connection
    public void freeConnection(String name, Connection conn) {
        DBConnectionPool pool = (DBConnectionPool) pools.get(name);
        if (pool != null)
            pool.freeConnection(conn);
        Log.out("One Connection of " + name + " was freed");
    }

    // Open Connection을 얻음. 현재 열린 connection이 없고, 최대 connection 개수가
    // 사용 중이 아닐 때는 새로운 connection을 생성. 현재 열린 connection이 없고,
    // 최대 connection 개수가 사용 중일 때 기본 대기 시간을 기다림
    // @param name : Pool name
    // @return Connection : The connection or null
    public Connection getConnection(String name) {
        DBConnectionPool pool = (DBConnectionPool) pools.get(name);
        if (pool != null)
            return pool.getConnection(10);
        return null;
    }

    // Connection Pool을 생성
    // @param poolName : 생성할 Pool Name
    // @param url : DB URL
    // @param user : DB UserID
    // @param password : DB Password
    private void createPools(String name, String URL, String userID, String password,
                             int maxConn, int initConn, int waitTime) {
        DBConnectionPool pool = new DBConnectionPool(name, URL, userID, password, maxConn, initConn, waitTime);
        pools.put(name, pool);
        Log.out("Initialized pool " + name);
    }

    // 초기화 작업
    public void init(String name, String driver, String URL, String userID, String password,
                     int maxConn, int initConn, int waitTime) {
        loadDrivers(driver);
        createPools(name, URL, userID, password, maxConn, initConn, waitTime);
    }

    // JDBC Driver Loading
    // @param driverClassName : 사용하고자 하는 DB의 JDBC 드라이버
    private void loadDrivers(String driverClassName) {
        try {
            Class.forName(driverClassName);
            drivers.addElement(driverClassName);
            Log.out("Registered JDBC driver " + driverClassName);
        } catch (ClassNotFoundException e) {
            Log.err(e, "Can't register JDBC driver: " + driverClassName);
        }
    }

    public Hashtable<String, DBConnectionPool> getPools() {
        return pools;
    }

    public int getDriverNumber() {
        return drivers.size();
    }
}
