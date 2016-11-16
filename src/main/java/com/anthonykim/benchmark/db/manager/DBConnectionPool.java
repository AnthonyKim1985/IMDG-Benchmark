package com.anthonykim.benchmark.db.manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Date;
import java.util.Vector;

public class DBConnectionPool {
	// 현재 사용 중인 Connection 개수
	private int checkedOut;

	// Free Connection List
	private Vector<Connection> freeConnections = new Vector<Connection>();

	// Connection 최대 개수
	private int maxConn;

	// Connection 초기 개수
	private int initConn;

	// Waiting time (pool에 connection이 없을때 기다리는 최대시간)
	private int maxWait;

	// Connection Pool Name
	private String name;

	// DB Password
	private String password;

	// DB URL
	private String URL;

	// DB UserID
	private String userID;

	public DBConnectionPool(
			String name, String URL, String userID, String password,
			int maxConn, int initConn, int waitTime) {
		this.name = name;
		this.URL = URL;
		this.userID = userID;
		this.password = password;
		this.maxConn = maxConn;
		this.initConn = initConn;
		this.maxWait = waitTime;

		for (int i=0; i<this.initConn; i++)
			freeConnections.addElement(newConnection());
	}
	
	// Connection 반납
	// @param con : 반납할 Connection 
	public synchronized void freeConnection(Connection conn) {
		freeConnections.addElement(conn);
		checkedOut--;
		
		// Connection을 얻기 위해 대기하고 있는 thread에 알림
		notifyAll();
	}
	
	// Connection을 얻음
	@SuppressWarnings("resource")
	public synchronized Connection getConnection() {
		Connection conn = null;
		
		// Connection이 Free List에 있으면 List의 처음 것을 얻음
		if (!freeConnections.isEmpty()) {
			conn = (Connection) freeConnections.firstElement();
			freeConnections.removeElement(0);
			
			try {
				if (conn.isClosed()) {
					Log.err("Removed bad connection from " + name);
					conn = getConnection();
				}
			} catch (SQLException e) {
				// connection이 정상적이지 않을시, 다시 요구한다.
				Log.err("Removed bad connection from " + name);
				conn = getConnection();
			}
		}
		else if (maxConn == 0 || checkedOut < maxConn) {
			conn = newConnection();
		}
		
		if (conn != null) checkedOut++;
		
		return conn;
	}
	
	// Connection을 얻음
	// @param timeout : Connection을 얻기 위한 최대 기다림 시간
	public synchronized Connection getConnection(long timeout) {
		long startTime = new Date().getTime();
		Connection conn = null;
		while ((conn = getConnection()) == null) {
			try {
				wait(timeout * maxWait);
			} catch (InterruptedException e) {
				if ((new Date().getTime() - startTime) >= timeout)
					return null; // 타임아웃을 초과
			}
		}
		return conn;
	}

	private Connection newConnection() {
		Connection conn = null;
		try {
			if (userID == null) {
				conn = DriverManager.getConnection(URL);
			}
			else {
				conn = DriverManager.getConnection(URL, userID, password);
			}
			Log.out("Created a new connection in pool " + name);
		} catch (SQLException e) {
			StringBuffer sb = new StringBuffer();
			sb.append("Can't create a new connection for ");
			sb.append(URL);
			sb.append(" user: ");
			sb.append(userID);
			sb.append(" password: ");
			sb.append(password);
			e.printStackTrace();
			Log.err(e, sb.toString());
			return null;
		}
		return conn;
	}
}
