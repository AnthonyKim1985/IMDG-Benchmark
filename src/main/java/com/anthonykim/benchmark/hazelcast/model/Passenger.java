package com.anthonykim.benchmark.hazelcast.model;

import com.anthonykim.benchmark.hazelcast.serializer.PassengerDataSerializableFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class Passenger implements IdentifiedDataSerializable {
    private long idx;
    private long idx_origin;
    private String passenger_username;
    private double latitude;
    private double longitude;

    public Passenger() {
    }

    public Passenger(long idx, long idx_origin, String passenger_username,
                     double latitude, double longitude) {
        super();
        this.idx = idx;
        this.idx_origin = idx_origin;
        this.passenger_username = passenger_username;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Passenger(String value) {
        String[] values = value.split(",");

        this.idx = Long.parseLong(values[0]);
        this.idx_origin = Long.parseLong(values[1]);
        this.passenger_username = new String(values[2]);
        this.latitude = Double.parseDouble(values[3]);
        this.longitude = Double.parseDouble(values[4]);
    }


    @Override
    public void readData(ObjectDataInput in) throws IOException {
        idx = in.readLong();
        idx_origin = in.readLong();
        passenger_username = in.readUTF();
        latitude = in.readDouble();
        longitude = in.readDouble();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(idx);
        out.writeLong(idx_origin);
        out.writeUTF(passenger_username);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
    }

    @Override
    public int getFactoryId() {
        return PassengerDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getId() {
        return PassengerDataSerializableFactory.PASSENGER_TYPE;
    }

    public long getIdx() {
        return idx;
    }

    public void setIdx(long idx) {
        this.idx = idx;
    }

    public long getIdx_origin() {
        return idx_origin;
    }

    public void setIdx_origin(long idx_origin) {
        this.idx_origin = idx_origin;
    }

    public String getPassenger_username() {
        return passenger_username;
    }

    public void setPassenger_username(String passenger_username) {
        this.passenger_username = passenger_username;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
