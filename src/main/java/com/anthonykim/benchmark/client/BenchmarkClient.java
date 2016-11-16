package com.anthonykim.benchmark.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.anthonykim.benchmark.util.PropertyManager;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.log4j.BasicConfigurator;


public class BenchmarkClient {
	private static final String updateDriverParams = "/?index=6&latitude=36.6289977000&longitude=127.4531764000&mode=1";
	private static final String selectDriverParams = "/?index=0&latitude=37.5064646000&longitude=127.1124789000&mode=2";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		if (args.length != 2) {
			System.err.println("Usage: java -jar BenchmarkClient.jar [proeprties_file_path] [query_mode]");
			System.exit(-1);
		} 
		
		List<String> targetURLs = new ArrayList<String>();
		
		Properties props = PropertyManager.loadProperties(args[0]);
		String [] serverAddresses = props.getProperty("serverAddresses").replaceAll("[\t ]", "").split(",");
		
		int nThread = Integer.parseInt(props.getProperty("nThread"));
		int defaultPort = Integer.parseInt(props.getProperty("defaultPort"));
		int testCase = Integer.parseInt(props.getProperty("testCase"));
		
		switch (Integer.parseInt(args[1])) {
		case 1:
			for (String serverAddress : serverAddresses)
				for (int i=0; i<nThread; i++)
					targetURLs.add(new String("http://" + serverAddress + ":" + (defaultPort + i) + updateDriverParams));
			break;
		case 2:
			for (String serverAddress : serverAddresses)
				for (int i=0; i<nThread; i++)
					targetURLs.add(new String("http://" + serverAddress + ":" + (defaultPort + i) + selectDriverParams));
			break;
		default:
			System.err.println("The argument [query_mode] has a value 1 or 2.");
			System.exit(-1);
		}
		
		ExecutorService executorSerivce = Executors.newFixedThreadPool(nThread);
		Iterator<String> iterator = targetURLs.iterator();
		while (iterator.hasNext())
			executorSerivce.submit(new Task(iterator.next(), testCase));
	}
	
	private static class Task implements Callable<Long> {
		private String url;
		private int testCase;
		
		public Task(String url, int testCase) {
			this.url = url;
			this.testCase = testCase;
			System.out.println(Thread.currentThread().toString() + ": " + url);
		}
		
		@Override
		public Long call() throws Exception {
			DefaultExecutor executor = new DefaultExecutor();
			CommandLine cmdLine = CommandLine.parse("ab -n " + testCase + " " + url);
			executor.execute(cmdLine);
			System.out.println(Thread.currentThread().toString() + ": " + "exit");
			return 0L;
		}
	}
}
