package com.anthonykim.benchmark.util;

public class DistanceConverter {
	public static double convertGspToMeter(double lat_from, double long_from, double lat_to, double long_to) {  
		double dist = 0.0D;
		double theta = long_from - long_to;
		
		dist = Math.sin(deg2rad(lat_from)) * Math.sin(deg2rad(lat_to)) + Math.cos(deg2rad(lat_from)) * Math.cos(deg2rad(lat_to)) * Math.cos(deg2rad(theta));  
		dist = Math.acos(dist);  
		dist = rad2deg(dist);  

		dist = dist * 60 * 1.1515;   
		dist = dist * 1.609344;  
		dist = dist * 1000.0;  

		return dist;  
	}  

	private static double deg2rad(double deg) {  
		return (double) (deg * Math.PI / (double) 180D);  
	}  

	private static double rad2deg(double rad) {  
		return (double) (rad * (double) 180D / Math.PI);  
	} 
}
