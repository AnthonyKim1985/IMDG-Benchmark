package com.anthonykim.benchmark.hazelcast.serializer;

import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class PassengerDataSerializableFactory implements DataSerializableFactory {
	public static final int FACTORY_ID = 0x00850628;
	public static final int PASSENGER_TYPE = 0x00850628;
	
	@Override
	public IdentifiedDataSerializable create(int typeId) {
		if (typeId == PASSENGER_TYPE)
			return new Driver();
		return null;
	}
}