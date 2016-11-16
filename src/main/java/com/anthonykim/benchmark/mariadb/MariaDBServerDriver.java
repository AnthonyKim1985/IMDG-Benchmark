package com.anthonykim.benchmark.mariadb;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.anthonykim.benchmark.util.PropertyManager;

import org.apache.log4j.BasicConfigurator;

public class MariaDBServerDriver {
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		if (args.length != 1) {
			System.err.println("Usage: java -jar MariaDBServer.jar [proeprties_file_path]");
			System.exit(-1);
		}
		
		Properties propsFile = PropertyManager.loadProperties(args[0]);
		
		int defaultPort = Integer.parseInt(propsFile.getProperty("defaultPort"));
		final int nThread = Integer.parseInt(propsFile.getProperty("nThread"));
		ExecutorService service = Executors.newFixedThreadPool(nThread);
		
		//ConnectionManager connMgr = new MySQLConnectionManager(args[0]);
		
		for (int i=0; i<nThread; i++)
			service.submit(new HttpServerForMariaDB(defaultPort++, propsFile));
	}
}