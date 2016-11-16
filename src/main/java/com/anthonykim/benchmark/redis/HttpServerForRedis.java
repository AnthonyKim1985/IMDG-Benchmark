package com.anthonykim.benchmark.redis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

import java.util.concurrent.Callable;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.anthonykim.benchmark.Benchmark;
import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.anthonykim.benchmark.hazelcast.model.Passenger;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import redis.clients.jedis.HostAndPort;

import redis.clients.jedis.JedisCluster;

import redis.clients.jedis.Protocol;


public class HttpServerForRedis extends AbstractHandler implements Benchmark, Callable<Long> {
	private Logger logger = Logger.getLogger(getClass());
	private String driverSelectQuery;
	private String passengerSelectQuery;
	private Server httpServer;
	private int httpPort;
	
	private final double latitudeAlpha = 0.019145D;
	private final double longitudeAlpha = 0.02326D;
	
	private final int UPDATE_MODE = 1;
	private final int SELECT_MODE = 2;
	private final int UPDATE_RAND_MODE = 3;
	private final int SELECT_RAND_MODE = 4;
	
	private JedisCluster jedisCluster;
//	private Jedis jedis;

	public HttpServerForRedis(Properties props, int port) {
		logger.info("Initialize an instance of HttpServerForRedis...");
		driverSelectQuery = props.getProperty("driverSelectQuery");
		passengerSelectQuery = props.getProperty("passengerSelectQuery");
		
		this.httpPort = port;
		QueuedThreadPool threadPool = new QueuedThreadPool(100, 10);
		httpServer = new Server(threadPool);
		
		/*
		 * Redis Single code
		 */
//		jedis = new Jedis("210.125.146.105", Protocol.DEFAULT_PORT);
//		jedis.connect();
		
		/*
		 * Redis Multi code
		 */
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//		jedisClusterNodes.add(new HostAndPort("210.125.146.97", Protocol.DEFAULT_PORT));
//		jedisClusterNodes.add(new HostAndPort("210.125.146.98", Protocol.DEFAULT_PORT));
		jedisClusterNodes.add(new HostAndPort("210.125.146.105", Protocol.DEFAULT_PORT));
		jedisClusterNodes.add(new HostAndPort("210.125.146.106", Protocol.DEFAULT_PORT));
		jedisClusterNodes.add(new HostAndPort("210.125.146.107", Protocol.DEFAULT_PORT));
		jedisCluster = new JedisCluster(jedisClusterNodes);
	}
	
	public void startupRedisServer(Connection conn, int loadData) throws Exception {
		if (loadData == 1) {
			loadDriver(conn);
			loadPassenger(conn);
		}
		startHttpServer();
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
			/*
			 * Redis Multi
			 */
			jedisCluster.set(String.valueOf(taxiDriver.getIdx()), taxiDriver.toString());
			
			/*
			 * Redis Single
			 */
			//jedis.set(String.valueOf(taxiDriver.getIdx()), taxiDriver.toString());
		}
		rs.close();
		pstmt.close();
	}
	
	private void loadPassenger(Connection conn) throws Exception {
		PreparedStatement pstmt = conn.prepareStatement(passengerSelectQuery);
		ResultSet rs = pstmt.executeQuery();
		
		while (rs.next()) {
			Passenger passenger = new Passenger();
			passenger.setIdx(rs.getLong(1));
			passenger.setIdx_origin(rs.getLong(2));
			passenger.setPassenger_username(rs.getString(3));
			passenger.setLatitude(rs.getDouble(4));
			passenger.setLongitude(rs.getDouble(5));
			/*
			 * Redis Multi
			 */
			jedisCluster.set(String.valueOf(passenger.getIdx()) + 10000, passenger.toString());
			
			/*
			 * Redis Single
			 */
			//jedis.set(String.valueOf(passenger.getIdx()) + 10000, passenger.toString());
		}
		rs.close();
		pstmt.close();
	}



	@Override
	public List<Driver> selectDriver() {
		List<Driver> driverList = new ArrayList<Driver>();
		Random rand = new Random();
		long index = rand.nextInt(8000) + 10001;
		
		Passenger passenger = new Passenger(jedisCluster.get(String.valueOf(index)));
		//Passenger passenger = new Passenger(jedis.get(String.valueOf(index)));
		double lat = passenger.getLatitude();
		double lng = passenger.getLongitude();
		
		/*
		 * Redis Multi
		 */
		long key = 1L;
		String value = null;

		while (true) {
			if ((value = jedisCluster.get(String.valueOf(key))) == null) break;
			
			String [] values = value.split(",");
			
			if (values.length != 6)
				continue;
			
			double lat_to = Double.parseDouble(values[2]);
			double long_to = Double.parseDouble(values[3]);

			if (lat_to >= (lat - latitudeAlpha) && lat_to <= (lat + latitudeAlpha))
				if (long_to >= (lng - longitudeAlpha) && long_to <= (lng + longitudeAlpha))
					driverList.add(new Driver(value));
			
			key++;
		}
		
		/*
		 * Redis Single
		 */
//		synchronized (this) {
//			Set<String> values = jedis.keys("*");
//			for (String value : values) {
//				String eachValue = jedis.get(value);
//				String [] eachValues = eachValue.split(",");
//				
//				if (eachValue.length() != 6)
//					continue;
//
//				double lat_to = Double.parseDouble(eachValues[2]);
//				double long_to = Double.parseDouble(eachValues[3]);
//				
//				if (lat_to >= (lat - latitudeAlpha) && lat_to <= (lat + latitudeAlpha))
//					if (long_to >= (lng - longitudeAlpha) && long_to <= (lng + longitudeAlpha))
//						driverList.add(new Driver(eachValue));
//			}
//		}
		return driverList;
	}

	@Override
	public boolean updateDriver() {
		Random rand = new Random();
		long idx = rand.nextInt(1800) + 1;
		/*
		 * Redis Multi
		 */
		jedisCluster.set(String.valueOf(idx), jedisCluster.get(String.valueOf(idx)));
		
		/*
		 * Redis Single
		 */
//		synchronized (this) {
//			jedis.set(String.valueOf(idx), jedis.get(String.valueOf(idx)));
//		}
		return true;
	}

	@Override
	public List<Driver> selectDriver(double lat, double lng) {
		List<Driver> driverList = new ArrayList<Driver>();
		
		/*
		 * Redis Multi
		 */
		long key = 1L;
		String value = null;

		while (true) {
			if ((value = jedisCluster.get(String.valueOf(key))) == null) break;
			
			String [] values = value.split(",");
			
			if (values.length != 6)
				continue;
			
			double lat_to = Double.parseDouble(values[2]);
			double long_to = Double.parseDouble(values[3]);

			if (lat_to >= (lat - latitudeAlpha) && lat_to <= (lat + latitudeAlpha))
				if (long_to >= (lng - longitudeAlpha) && long_to <= (lng + longitudeAlpha))
					driverList.add(new Driver(value));
			
			key++;
		}
		
		/*
		 * Redis Single
		 */
//		synchronized (this) {
//			Set<String> values = jedis.keys("*");
//			for (String value : values) {
//				String eachValue = jedis.get(value);
//				String [] eachValues = eachValue.split(",");
//				
//				if (eachValue.length() != 6)
//					continue;
//
//				double lat_to = Double.parseDouble(eachValues[2]);
//				double long_to = Double.parseDouble(eachValues[3]);
//				
//				if (lat_to >= (lat - latitudeAlpha) && lat_to <= (lat + latitudeAlpha))
//					if (long_to >= (lng - longitudeAlpha) && long_to <= (lng + longitudeAlpha))
//						driverList.add(new Driver(eachValue));
//			}
//		}
		return driverList;
	}

	@Override
	public boolean updateDriver(long idx, double lat, double lng) {
		/*
		 * Redis Multi
		 */
		jedisCluster.set(String.valueOf(idx), jedisCluster.get(String.valueOf(idx)));
		
		/*
		 * Redis Single
		 */
//		synchronized (this) {
//			jedis.set(String.valueOf(idx), jedis.get(String.valueOf(idx)));
//		}
		return true;
	}

	@Override
	public Long call() throws Exception {
		startHttpServer();
		return 0L;
	}

	private void startHttpServer() throws Exception {
		ServerConnector http = new ServerConnector(httpServer);
		http.setAcceptQueueSize(4096);
		http.setIdleTimeout(30000);
		http.setPort(httpPort);
		
		httpServer.addConnector(http);
		httpServer.setHandler(this);
		httpServer.start();
		httpServer.join();
	}

	@Override
	public void handle(String target, Request baseRequest, 
			HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		response.setContentType("text/html; charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

		final Map<String, String[]> parameterMap = request.getParameterMap();
		final long index = Long.parseLong(parameterMap.get("index")[0]);
		final double latitude = Double.parseDouble(parameterMap.get("latitude")[0]);
		final double longitude = Double.parseDouble(parameterMap.get("longitude")[0]);
		final int mode = Integer.parseInt(parameterMap.get("mode")[0]);

		//long start = System.currentTimeMillis();
		if (mode == UPDATE_MODE) {
			response.getWriter().println(updateDriver(index, latitude, longitude));
		} 
		else if (mode == SELECT_MODE) {
			response.getWriter().println(selectDriver(latitude, longitude));
		}
		else if (mode == UPDATE_RAND_MODE) {
			response.getWriter().println(updateDriver());
		}
		else if (mode == SELECT_RAND_MODE) {
			response.getWriter().println(selectDriver());
		}

		response.setStatus(HttpServletResponse.SC_OK);
		response.flushBuffer();

		//System.out.println(System.currentTimeMillis() - start + "ms");
	}
}
