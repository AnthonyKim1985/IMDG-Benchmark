package com.anthonykim.benchmark.redis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Properties;

import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.anthonykim.benchmark.hazelcast.model.Passenger;
import redis.clients.jedis.Jedis;


public class RedisServer {
	private String driverSelectQuery;
	private String passengerSelectQuery;
	private String dbServer;
	private Jedis driverRedis;
	private Jedis passengerMap;
	
	public RedisServer(Jedis driverRedis, Properties props) {
		passengerSelectQuery = props.getProperty("passengerSelectQuery");
		driverSelectQuery = props.getProperty("driverSelectQuery");
		dbServer = props.getProperty("dbServer");
		this.driverRedis = driverRedis;
	}
	
	public void startupRedisServer(Connection conn, int loadData) throws Exception {
		if (loadData == 1) {
			loadDriver(conn);
			//passengerMap = loadPassenger(conn, passengerKeys);
		}
	}

	private void loadDriver(Connection conn) throws Exception {
		PreparedStatement pstmt = conn.prepareStatement(driverSelectQuery);
		ResultSet rs = pstmt.executeQuery();
		
		while (rs.next()) {
			Driver taxiDriver = new Driver();
			taxiDriver.setIdx(rs.getInt(1));
			taxiDriver.setDriver(rs.getString(2));
			taxiDriver.setLat_from(rs.getDouble(3));
			taxiDriver.setLong_from(rs.getDouble(4));
			taxiDriver.setLat_to(rs.getDouble(5));
			taxiDriver.setLong_to(rs.getDouble(6));
			System.out.println(taxiDriver);
			driverRedis.set(String.valueOf(taxiDriver.getIdx()), taxiDriver.toString());
		}
		rs.close();
		pstmt.close();
	}
	
	private Jedis loadPassenger(Connection conn) throws Exception {
		Jedis passengerRedis = new Jedis(dbServer);
		PreparedStatement pstmt = conn.prepareStatement(passengerSelectQuery);
		ResultSet rs = pstmt.executeQuery();
		passengerRedis.connect();
		
		while (rs.next()) {
			Passenger taxiPassenger = new Passenger();
			taxiPassenger.setIdx(rs.getInt(1));
			taxiPassenger.setIdx_origin(rs.getInt(2));
			taxiPassenger.setPassenger_username(rs.getString(3));
			taxiPassenger.setLatitude(rs.getDouble(4));
			taxiPassenger.setLongitude(rs.getDouble(5));
			passengerRedis.set(taxiPassenger.getPassenger_username(), taxiPassenger.toString());
		}
		rs.close();
		pstmt.close();
		
		return passengerRedis;
	}
}