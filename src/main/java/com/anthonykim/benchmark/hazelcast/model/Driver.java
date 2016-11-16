package com.anthonykim.benchmark.hazelcast.model;

import com.anthonykim.benchmark.hazelcast.serializer.DriverDataSerializableFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class Driver implements IdentifiedDataSerializable {
    private long idx;
    private String driver;
    private double lat_from;
    private double long_from;
    private double lat_to;
    private double long_to;

    public Driver() {
    }

    public Driver(long idx, String driver, double lat_from, double long_from,
                  double lat_to, double long_to) {
        super();
        this.idx = idx;
        this.driver = driver;
        this.lat_from = lat_from;
        this.long_from = long_from;
        this.lat_to = lat_to;
        this.long_to = long_to;
    }

    public Driver(String value) {
        String[] values = value.split(",");

        this.idx = Long.parseLong(values[0]);
        this.driver = new String(values[1]);
        this.lat_from = Double.parseDouble(values[2]);
        this.long_from = Double.parseDouble(values[3]);
        this.lat_to = Double.parseDouble(values[4]);
        this.long_to = Double.parseDouble(values[5]);
    }

    @Override
    public String toString() {
        return idx + "," + driver + "," + lat_from + "," + long_from + "," + lat_to + "," + long_from;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        idx = in.readLong();
        driver = in.readUTF();
        lat_from = in.readDouble();
        long_from = in.readDouble();
        lat_to = in.readDouble();
        long_to = in.readDouble();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(idx);
        out.writeUTF(driver);
        out.writeDouble(lat_from);
        out.writeDouble(long_from);
        out.writeDouble(lat_to);
        out.writeDouble(long_to);
    }

    @Override
    public int getFactoryId() {
        return DriverDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getId() {
        return DriverDataSerializableFactory.DRIVER_TYPE;
    }

    public long getIdx() {
        return idx;
    }

    public void setIdx(long idx) {
        this.idx = idx;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public double getLat_from() {
        return lat_from;
    }

    public void setLat_from(double lat_from) {
        this.lat_from = lat_from;
    }

    public double getLong_from() {
        return long_from;
    }

    public void setLong_from(double long_from) {
        this.long_from = long_from;
    }

    public double getLat_to() {
        return lat_to;
    }

    public void setLat_to(double lat_to) {
        this.lat_to = lat_to;
    }

    public double getLong_to() {
        return long_to;
    }

    public void setLong_to(double long_to) {
        this.long_to = long_to;
    }
}