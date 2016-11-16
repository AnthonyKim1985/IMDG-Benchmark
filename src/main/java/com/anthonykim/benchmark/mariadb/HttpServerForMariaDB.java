package com.anthonykim.benchmark.mariadb;

import com.anthonykim.benchmark.Benchmark;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import com.anthonykim.benchmark.hazelcast.model.Driver;

public class HttpServerForMariaDB implements Benchmark, Callable<Long> {
    private Logger logger = Logger.getLogger(getClass());
    private Server httpServer;
    private Properties props;
    private int httpPort;
    private Connection conn;

    private final String query = "select a.didx, a.driver, a.lat_from, a.long_from " +
            "from (select p.idx, p.passenger_username, p.latitude, p.longitude, d.idx as didx, d.driver, d.lat_from, d.long_from, " +
            "(6371 * acos(cos(radians(p.latitude)) * cos(radians(d.lat_from )) * cos(radians(d.long_from) - radians(p.longitude)) + sin(radians(p.latitude)) * sin(radians(d.lat_from)))) " +
            "AS distance from DangolTaxi.passenger p , DangolTaxi.driver d where p.idx = ?) as a where distance < 1;";

    public HttpServerForMariaDB(int port, Properties props) {
        logger.info("Initialize an instance of HttpServerForMariaDB...");
        QueuedThreadPool threadPool = new QueuedThreadPool(100, 10);
        httpServer = new Server(threadPool);
        this.httpPort = port;

        String mySqlDriver = "com.mysql.jdbc.Driver";
        try {
            Class.forName(mySqlDriver);
        } catch (ClassNotFoundException e) {
        }

        String mySqlURL = "jdbc:mysql://" + props.getProperty("dbServer") + ":" + props.getProperty("port") + "/" + props.getProperty("dbName");
        String user = props.getProperty("userID");
        String password = props.getProperty("password");
        try {
            this.conn = DriverManager.getConnection(mySqlURL, user, password);
        } catch (SQLException e) {
        }

        this.props = props;
    }


    @Override
    public List<Driver> selectDriver() {
        List<Driver> driverList = new ArrayList<Driver>();

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        Random rand = new Random();
        int rand_index = rand.nextInt(8000) + 1;

        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, rand_index);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Driver aDriver = new Driver();
                aDriver.setIdx(rs.getLong(1));
                aDriver.setDriver(rs.getString(2));
                aDriver.setLat_from(rs.getDouble(3));
                aDriver.setLong_from(rs.getDouble(4));
                driverList.add(aDriver);
            }
        } catch (SQLException e) {
            logger.debug(e.getMessage());
        } finally {
            if (pstmt != null) try {
                pstmt.close();
            } catch (SQLException e) {
            }
            if (rs != null) try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        return driverList;
    }

    @Override
    public boolean updateDriver() {
        return true;
    }

    @Override
    public List<Driver> selectDriver(double lat, double lng) {
        List<Driver> driverList = new ArrayList<Driver>();

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            pstmt = conn.prepareStatement(query);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Driver aDriver = new Driver();
                aDriver.setIdx(rs.getLong(1));
                aDriver.setDriver(rs.getString(2));
                aDriver.setLat_from(rs.getDouble(3));
                aDriver.setLong_from(rs.getDouble(4));
                driverList.add(aDriver);
            }
        } catch (SQLException e) {
            logger.debug(e.getMessage());
        } finally {
            if (pstmt != null) try {
                pstmt.close();
            } catch (SQLException e) {
            }
            if (rs != null) try {
                rs.close();
            } catch (SQLException e) {
            }
        }
        return driverList;
    }

    @Override
    public boolean updateDriver(long idx, double lat, double lng) {
        final String driverUpdateQuery = props.getProperty("driverUpdateQuery");

        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(driverUpdateQuery);
            pstmt.setDouble(1, lat);
            pstmt.setDouble(2, lng);
            pstmt.setLong(3, idx);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.debug(e.getMessage());
        } finally {
            if (pstmt != null) try {
                pstmt.close();
            } catch (SQLException e) {
            }
        }
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
        httpServer.setHandler(new HttpServerHandler());
        httpServer.start();
        httpServer.join();
    }

    private class HttpServerHandler extends AbstractHandler {
        private final int UPDATE_MODE = 1;
        private final int SELECT_MODE = 2;
        private final int UPDATE_RAND_MODE = 3;
        private final int SELECT_RAND_MODE = 4;

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
            } else if (mode == SELECT_MODE) {
                response.getWriter().println(selectDriver(latitude, longitude));
            } else if (mode == UPDATE_RAND_MODE) {
                response.getWriter().println(updateDriver(index, latitude, longitude));
            } else if (mode == SELECT_RAND_MODE) {
                response.getWriter().println(selectDriver());
            }

            response.setStatus(HttpServletResponse.SC_OK);
            response.flushBuffer();
            //System.out.println(System.currentTimeMillis() - start + "ms");
        }
    }
}