package com.anthonykim.benchmark.hazelcast;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.anthonykim.benchmark.hazelcast.model.Passenger;
import com.anthonykim.benchmark.hazelcast.serializer.DriverDataSerializableFactory;
import com.anthonykim.benchmark.hazelcast.serializer.PassengerDataSerializableFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastServer {
    public static String driverMapName;
    public static String passengerMapName;
    private static String driverSelectQuery;
    private static String passengerSelectQuery;


    private static HazelcastServer instance;
    private HazelcastInstance hzInst;

    private HazelcastServer(Properties props) throws Exception {
        Config hzConfig = new Config();
        hzConfig.setProperty("hazelcast.logging.type", "log4j");

        driverMapName = props.getProperty("driverMapName");
        passengerMapName = props.getProperty("passengerMapName");
        driverSelectQuery = props.getProperty("driverSelectQuery");
        passengerSelectQuery = props.getProperty("passengerSelectQuery");

        SerializationConfig driverSerializationConfig = hzConfig.getSerializationConfig();
        driverSerializationConfig.addDataSerializableFactory(DriverDataSerializableFactory.FACTORY_ID, new DriverDataSerializableFactory());

        SerializationConfig passengerSerializationConfig = hzConfig.getSerializationConfig();
        passengerSerializationConfig.addDataSerializableFactory(PassengerDataSerializableFactory.FACTORY_ID, new PassengerDataSerializableFactory());

        setNetworkConfig(props, hzConfig.getNetworkConfig());

        hzConfig.addMapConfig(getMapConfig(driverMapName));
        hzConfig.addMapConfig(getMapConfig(passengerMapName));

        hzInst = Hazelcast.newHazelcastInstance(hzConfig);
    }

    private void setNetworkConfig(Properties props, NetworkConfig networkConfig) {
        networkConfig.setPort(Integer.parseInt(props.getProperty("hazelcast_port_number")));
        networkConfig.setPortAutoIncrement(true);

        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);

        String member = props.getProperty("hazelcast_ip_member");
        String[] members = member.split(",");

        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        for (String specificMember : members)
            tcpIpConfig.addMember(specificMember);
        tcpIpConfig.setRequiredMember(props.getProperty("hazelcast_server"));
        tcpIpConfig.setEnabled(true);

        networkConfig.getInterfaces().setEnabled(true).addInterface(props.getProperty("hazelcast_ip_range"));
    }

    public void startupHazelcastServer(Connection conn, int loadData) throws Exception {
        if (loadData == 1) {
            loadDriver(conn);
            loadPassenger(conn);
        }
    }

    private MapConfig getMapConfig(String mapName) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.OFFHEAP);
        mapConfig.setAsyncBackupCount(1);
        mapConfig.setBackupCount(1);
        mapConfig.setReadBackupData(true);

        return mapConfig;
    }

    public static synchronized HazelcastServer getInstance(Properties props) throws Exception {
        if (instance == null)
            return new HazelcastServer(props);
        return instance;
    }

    private void loadDriver(Connection conn) throws Exception {
        PreparedStatement pstmt = conn.prepareStatement(driverSelectQuery);
        ResultSet rs = pstmt.executeQuery();

        final IMap<Long, Driver> driverMap = hzInst.getMap(driverMapName);
        while (rs.next()) {
            Driver driver = new Driver();
            driver.setIdx(rs.getInt(1));
            driver.setDriver(rs.getString(2));
            driver.setLat_from(rs.getDouble(3));
            driver.setLong_from(rs.getDouble(4));
            driver.setLat_to(rs.getDouble(5));
            driver.setLong_to(rs.getDouble(6));
            driverMap.set(driver.getIdx(), driver);
        }
        rs.close();
        pstmt.close();
    }

    private void loadPassenger(Connection conn) throws Exception {
        PreparedStatement pstmt = conn.prepareStatement(passengerSelectQuery);
        ResultSet rs = pstmt.executeQuery();

        final IMap<Long, Passenger> passengerMap = hzInst.getMap(passengerMapName);
        while (rs.next()) {
            Passenger passenger = new Passenger();
            passenger.setIdx(rs.getLong(1));
            passenger.setIdx_origin(rs.getLong(2));
            passenger.setPassenger_username(rs.getString(3));
            passenger.setLatitude(rs.getDouble(4));
            passenger.setLongitude(rs.getDouble(5));
            passengerMap.set(passenger.getIdx(), passenger);
        }
        rs.close();
        pstmt.close();
    }

}