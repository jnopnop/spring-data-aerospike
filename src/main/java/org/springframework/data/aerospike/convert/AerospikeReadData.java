/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Value object to carry data to be read in object conversion.
 *
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
public class AerospikeReadData {

	private final Key key;
	private final Map<String, Object> record;
	private final int expiration;
	private final int version;

	private AerospikeReadData(Key key, Map<String, Object> record, int expiration, int version) {
		this.key = key;
		this.record = record;
		this.expiration = expiration;
		this.version = version;
	}

	public static AerospikeReadData forRead(Key key, Record record) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(record, "Record must not be null");
		Assert.notNull(record.bins, "Record bins must not be null");

		return new AerospikeReadData(key, record.bins, record.getTimeToLive(), record.generation);
	}

	public Map<String, Object> getRecord() {
		return record;
	}

	public Key getKey() {
		return key;
	}

	public Object getValue(String key) {
		return record.get(key);
	}

	public int getExpiration() {
		return expiration;
	}

	public int getVersion() {
		return version;
	}
}
