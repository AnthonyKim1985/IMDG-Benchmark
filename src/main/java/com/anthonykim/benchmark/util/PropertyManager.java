package com.anthonykim.benchmark.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyManager {
	public static Properties loadProperties(String filePath) throws IOException {
		Properties props = new Properties();
		FileInputStream inputStream = new FileInputStream(filePath);
		props.load(inputStream);

		return props;
	}
}
