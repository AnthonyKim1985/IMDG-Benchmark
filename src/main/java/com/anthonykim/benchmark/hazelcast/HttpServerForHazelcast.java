package com.anthonykim.benchmark.hazelcast;

import com.anthonykim.benchmark.Benchmark;
import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.anthonykim.benchmark.hazelcast.model.Passenger;
import com.anthonykim.benchmark.hazelcast.serializer.DriverDataSerializableFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

public class HttpServerForHazelcast extends AbstractHandler implements Benchmark, Callable<Long> {
    private Logger logger = Logger.getLogger(getClass());

    private final double latitudeAlpha = 0.019145D;
    private final double longitudeAlpha = 0.02326D;

    private final int UPDATE_MODE = 1;
    private final int SELECT_MODE = 2;
    private final int UPDATE_RAND_MODE = 3;
    private final int SELECT_RAND_MODE = 4;

    private HazelcastInstance hzInst;
    private Server httpServer;
    private int httpPort;

    public HttpServerForHazelcast(Properties props, int httpPort,
                                  String address, int hzPort) {
        logger.info("Initialize an instance of HttpServerForHazelcast...");
        httpServer = new Server(new QueuedThreadPool(100, 10));
        this.httpPort = httpPort;

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        clientNetworkConfig.addAddress(address + ":" + hzPort);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        SerializationConfig driverSerializationConfig = clientConfig
                .getSerializationConfig();
        driverSerializationConfig.addDataSerializableFactory(
                DriverDataSerializableFactory.FACTORY_ID,
                new DriverDataSerializableFactory());

        hzInst = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Override
    public List<Driver> selectDriver() {
        final IMap<Long, Driver> driverMap = hzInst.getMap(HazelcastServer.driverMapName);
        final IMap<Long, Passenger> passengerMap = hzInst.getMap(HazelcastServer.passengerMapName);

        final int randSeed = passengerMap.size();

        Random rand = new Random();
        long index = rand.nextInt(randSeed) + 1;

        Passenger passenger = passengerMap.get(index);

        double lat = passenger.getLatitude();
        double lng = passenger.getLongitude();

        final String sqlPredicate = "lat_to > " + (lat - latitudeAlpha)
                + " AND lat_to < " + (lat + latitudeAlpha) + " AND "
                + "long_to > " + (lng - longitudeAlpha) + " AND long_to < "
                + (lng + longitudeAlpha);

        List<Driver> returnValue = (List<Driver>) driverMap
                .values(new SqlPredicate(sqlPredicate));

        return returnValue;
    }

    @Override
    public boolean updateDriver() {
        final IMap<Long, Driver> driverMap = hzInst.getMap(HazelcastServer.driverMapName);
        final int randSeed = driverMap.size();

        Random rand = new Random();
        long index = rand.nextInt(randSeed) + 1;

        driverMap.set(index, driverMap.get(index));
        return true;
    }

    @Override
    public List<Driver> selectDriver(double lat, double lng) {
        /*
		 * EntryObject e = new PredicateBuilder().getEntryObject(); Predicate
		 * latPredicate =
		 * e.get("lat_to").between(latitudeD.subtract(latitudeAlpha),
		 * latitudeD.add(latitudeAlpha)); Predicate predicate =
		 * e.get("long_to").between(longitudeD.subtract(longitudeAlpha),
		 * longitudeD.add(longitudeAlpha)).and(latPredicate);
		 */
        final IMap<Long, Driver> driverMap = hzInst
                .getMap(HazelcastServer.driverMapName);

        final String sqlPredicate = "lat_to > " + (lat - latitudeAlpha)
                + " AND lat_to < " + (lat + latitudeAlpha) + " AND "
                + "long_to > " + (lng - longitudeAlpha) + " AND long_to < "
                + (lng + longitudeAlpha);

        List<Driver> returnValue = (List<Driver>) driverMap
                .values(new SqlPredicate(sqlPredicate));
		/* debug */
        // for (Driver d : returnValue) {
        // System.out.println(d.getDriver());
        // }
		/* debug */

        return returnValue;
    }

    @Override
    public boolean updateDriver(long idx, double lat, double lng) {
        final IMap<Long, Driver> driverMap = hzInst
                .getMap(HazelcastServer.driverMapName);
        driverMap.set(idx, driverMap.get(idx));
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
                       HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        final Map<String, String[]> parameterMap = request.getParameterMap();

        final long index = Long.parseLong(parameterMap.get("index")[0]);
        final double latitude = Double
                .parseDouble(parameterMap.get("latitude")[0]);
        final double longitude = Double.parseDouble(parameterMap
                .get("longitude")[0]);
        final int mode = Integer.parseInt(parameterMap.get("mode")[0]);

        // long start = System.currentTimeMillis();


        if (mode == UPDATE_MODE) {
            response.getWriter().println(updateDriver(index, latitude, longitude));
        } else if (mode == SELECT_MODE) {
            response.getWriter().println(selectDriver(latitude, longitude));
        } else if (mode == UPDATE_RAND_MODE) {
            response.getWriter().println(updateDriver());
        } else if (mode == SELECT_RAND_MODE) {
            response.getWriter().println(selectDriver());
        }

        response.setStatus(HttpServletResponse.SC_OK);
        response.flushBuffer();

        // System.out.println(System.currentTimeMillis() - start + "ms");
    }

}