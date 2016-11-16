package com.anthonykim.benchmark.hazelcast;

import com.anthonykim.benchmark.util.PropertyManager;
import org.apache.log4j.BasicConfigurator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class HazelcastServerDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        for (int i = 0; i < args.length; i++)
            System.out.println(args[i]);

        if (args.length != 1) {
            System.err.println("Usage: java -jar HazelcastServer.jar [proeprties_file_path]");
            System.exit(-1);
        }

        Properties props = PropertyManager.loadProperties(args[0]);
        loadData(props);

        int defaultPort = Integer.parseInt(props.getProperty("defaultPort"));
        int hazelcast_port_number = Integer.parseInt(props.getProperty("hazelcast_port_number"));
        final int nThread = Integer.parseInt(props.getProperty("nThread"));

        String members[] = props.getProperty("hazelcast_ip_member").split(",");
        ExecutorService service = Executors.newFixedThreadPool(nThread);

        for (int i = 0; i < nThread; i++)
            service.submit(new HttpServerForHazelcast(props, defaultPort++, members[i], hazelcast_port_number++));

    }

    private static void loadData(Properties props) throws Exception {
        Connection conn = null;
        final String mySqlDriver = "com.mysql.jdbc.Driver";
        Class.forName(mySqlDriver);

        final String mySqlURL = "jdbc:mysql://" + props.getProperty("dbServer") + ":" + props.getProperty("port") + "/" + props.getProperty("dbName");
        final String user = props.getProperty("userID");
        final String password = props.getProperty("password");
        conn = DriverManager.getConnection(mySqlURL, user, password);

        final int loadData = Integer.parseInt(props.getProperty("loadData"));
        HazelcastServer hzServer = HazelcastServer.getInstance(props);
        hzServer.startupHazelcastServer(conn, loadData);

        conn.close();
    }
}