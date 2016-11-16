package com.anthonykim.benchmark.redis;

import com.anthonykim.benchmark.db.manager.ConnectionManager;
import com.anthonykim.benchmark.db.manager.MySQLConnectionManager;
import com.anthonykim.benchmark.util.PropertyManager;
import org.apache.log4j.BasicConfigurator;

import java.sql.Connection;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RedisServerDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        if (args.length != 1) {
            System.err.println("Usage: java -jar RedisServer.jar [proeprties_file_path]");
            System.exit(-1);
        }

        Properties props = PropertyManager.loadProperties(args[0]);
        ConnectionManager connMgr = new MySQLConnectionManager(args[0]);
        Connection conn = connMgr.getConnection();

        int loadData = Integer.parseInt(props.getProperty("loadData"));
        int defaultPort = Integer.parseInt(props.getProperty("defaultPort"));
        final int nThread = Integer.parseInt(props.getProperty("nThread"));

        ExecutorService service = Executors.newFixedThreadPool(nThread);

        HttpServerForRedis redis = new HttpServerForRedis(props, defaultPort);
        redis.startupRedisServer(conn, loadData);

        connMgr.freeConnection(conn);
    }
}