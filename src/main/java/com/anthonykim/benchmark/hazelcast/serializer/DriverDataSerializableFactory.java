package com.anthonykim.benchmark.hazelcast.serializer;

import com.anthonykim.benchmark.hazelcast.model.Driver;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class DriverDataSerializableFactory implements DataSerializableFactory {
	public static final int FACTORY_ID = 0x12345678;
	public static final int DRIVER_TYPE = 0x12345678;
	
	@Override
	public IdentifiedDataSerializable create(int typeId) {
		if (typeId == DRIVER_TYPE)
			return new Driver();
		return null;
	}
}
