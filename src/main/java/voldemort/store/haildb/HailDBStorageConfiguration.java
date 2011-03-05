package voldemort.store.haildb;

/*
 * Copyright 2008-2009 LinkedIn, Inc
 * Copyright 2010 Sunny Gleason
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

import com.g414.haildb.Database;
import com.g414.haildb.DatabaseConfiguration;
import com.g414.haildb.DatabaseConfiguration.LogFlushMode;

public class HailDBStorageConfiguration implements StorageConfiguration {
	public static final String TYPE_NAME = "haildb";
	public final Database database;

	public HailDBStorageConfiguration(VoldemortConfig ignored) {
		DatabaseConfiguration dbConfig = new DatabaseConfiguration();
		dbConfig.setFlushLogAtTrxCommitMode(LogFlushMode.ONCE_PER_SECOND);
		dbConfig.setIoCapacityIOPS(100000);

		this.database = new Database(dbConfig);
	}

	public StorageEngine<ByteArray, byte[], byte[]> getStore(String name) {
		return new HailDBStorageEngine(database, name);
	}

	public String getType() {
		return TYPE_NAME;
	}

	public void close() {
	}
}
