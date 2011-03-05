package voldemort.store.haildb;

import voldemort.store.AbstractStorageEngineTest;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;

import com.g414.haildb.Database;

public class HailDBStoreTest extends AbstractStorageEngineTest {
	private static final Database database = new Database();
	private HailDBStorageEngine engine = new HailDBStorageEngine(database,
			"vtest");

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		engine.truncate();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		engine.truncate();
	}

	public StorageEngine<ByteArray, byte[], byte[]> getStorageEngine() {
		return this.engine;
	}
}
