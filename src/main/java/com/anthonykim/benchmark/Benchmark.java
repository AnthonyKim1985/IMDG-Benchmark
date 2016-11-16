package com.anthonykim.benchmark;


import com.anthonykim.benchmark.hazelcast.model.Driver;

import java.util.List;

public interface Benchmark {
    List<Driver> selectDriver(double lat, double lng);

    boolean updateDriver(long idx, double lat, double lng);

    List<Driver> selectDriver();

    boolean updateDriver();
}