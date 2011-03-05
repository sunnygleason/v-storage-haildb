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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.PersistenceFailureException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.g414.haildb.ColumnAttribute;
import com.g414.haildb.ColumnType;
import com.g414.haildb.Cursor;
import com.g414.haildb.Database;
import com.g414.haildb.LockMode;
import com.g414.haildb.SearchMode;
import com.g414.haildb.TableBuilder;
import com.g414.haildb.TableDef;
import com.g414.haildb.Transaction;
import com.g414.haildb.TransactionLevel;
import com.g414.haildb.Tuple;
import com.g414.haildb.TupleBuilder;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.TransactionCallback;
import com.google.common.collect.Lists;

/**
 * A StorageEngine that uses HailDB for persistence
 */
public class HailDBStorageEngine implements
		StorageEngine<ByteArray, byte[], byte[]> {

	private final String schema = "vinno";
	private final String name;
	private final Database database;
	private final DatabaseTemplate dbt;
	private final TableDef def;

	public HailDBStorageEngine(Database database, String name) {
		this.name = name;
		this.database = database;
		this.dbt = new DatabaseTemplate(database);

		TableBuilder builder = new TableBuilder(schema + "/" + name);

		builder.addColumn("key_", ColumnType.VARBINARY, 200,
				ColumnAttribute.NOT_NULL);
		builder.addColumn("version_", ColumnType.VARBINARY, 200,
				ColumnAttribute.NOT_NULL);
		builder.addColumn("value_", ColumnType.BLOB, 0);

		builder.addIndex("PRIMARY", "key_", 0, true, true);
		builder.addIndex("PRIMARY", "version_", 0, true, true);

		this.def = builder.build();

		if (!tableExists()) {
			create();
		}
	}

	private boolean tableExists() {
		return this.database.tableExists(def);
	}

	public void destroy() {
		this.database.truncateTable(def);
	}

	public void create() {
		this.database.createDatabase(schema);
		this.database.createTable(def);
	}

	public void truncate() {
		this.database.truncateTable(def);
	}

	public ClosableIterator<ByteArray> keys() {
		Transaction t = this.database
				.beginTransaction(TransactionLevel.REPEATABLE_READ);
		Cursor c = t.openTable(def);
		c.first();

		return new KeysIterator(c, t);
	}

	public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
		Transaction t = this.database
				.beginTransaction(TransactionLevel.REPEATABLE_READ);
		Cursor c = t.openTable(def);
		c.first();

		return new EntriesIterator(c, t);
	}

	public List<Version> getVersions(ByteArray key) {
		return StoreUtils.getVersions(get(key, null));
	}

	public void close() throws PersistenceFailureException {
	}

	public Object getCapability(StoreCapabilityType capability) {
		throw new NoSuchCapabilityException(capability, getName());
	}

	public boolean delete(final ByteArray key, final Version maxVersion)
			throws PersistenceFailureException {
		StoreUtils.assertValidKey(key);

		TransactionCallback<Boolean> callback = new TransactionCallback<Boolean>() {

			public Boolean inTransaction(Transaction t) {
				Cursor c = t.openTable(def);
				c.lock(LockMode.LOCK_EXCLUSIVE);

				Tuple search = c
						.createClusteredIndexSearchTuple(new TupleBuilder(def)
								.addValues(key.get()));
				Tuple read = c.createClusteredIndexReadTuple();

				try {
					c.find(search, SearchMode.GE);
					boolean deletedSomething = false;
					boolean locked = false;

					while (c.hasNext()) {
						c.readRow(read);

						byte[] theKey = read.getBytes(0);
						byte[] version = read.getBytes(1);

						if (theKey != null && version != null
								&& Arrays.equals(key.get(), theKey)) {
							if (new VectorClock(version).compare(maxVersion) == Occured.BEFORE) {
								if (!locked) {
									c.setLockMode(LockMode.LOCK_EXCLUSIVE);
									locked = true;
								}

								c.deleteRow();
								deletedSomething = true;
							}
						} else {
							break;
						}

						c.next();
						read.clear();
					}

					return deletedSomething;
				} finally {
					read.delete();
					search.delete();
					c.close();
				}
			}
		};

		try {
			return this.dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
					callback);
		} catch (Exception e) {
			throw new PersistenceFailureException(e);
		}
	}

	public Map<ByteArray, List<Versioned<byte[]>>> getAll(
			Iterable<ByteArray> keys, Map<ByteArray, byte[]> transforms)
			throws VoldemortException {
		StoreUtils.assertValidKeys(keys);

		Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils
				.newEmptyHashMap(keys);

		Transaction t = this.database
				.beginTransaction(TransactionLevel.REPEATABLE_READ);
		Cursor c = t.openTable(def);
		c.setClusterAccess();
		c.setLockMode(LockMode.LOCK_SHARED);

		try {
			for (ByteArray key : keys) {
				Tuple search = c
						.createClusteredIndexSearchTuple(new TupleBuilder(def)
								.addValues(key.get()));
				Tuple read = c.createClusteredIndexReadTuple();

				try {
					c.find(search, SearchMode.GE);
					List<Versioned<byte[]>> found = Lists.newArrayList();

					while (c.hasNext()) {
						c.readRow(read);

						byte[] theKey = read.getBytes(0);
						byte[] versionBytes = read.getBytes(1);

						if (theKey != null && versionBytes != null
								&& Arrays.equals(key.get(), theKey)) {
							VectorClock version = new VectorClock(versionBytes);
							byte[] value = read.getBytes(2);
							found.add(new Versioned<byte[]>(value, version));
						} else {
							break;
						}

						c.next();
						read.clear();
					}

					if (found.size() > 0) {
						result.put(key, found);
					}
				} finally {
					search.delete();
					read.delete();
				}
			}

			return result;
		} finally {
			c.close();
			t.commit();
		}
	}

	public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
			throws VoldemortException {
		StoreUtils.assertValidKey(key);

		return StoreUtils.get(this, key, transforms);
	}

	public String getName() {
		return name;
	}

	public void put(ByteArray key, Versioned<byte[]> value, byte[] transformed)
			throws PersistenceFailureException {
		StoreUtils.assertValidKey(key);

		Transaction t = null;
		Cursor c = null;
		Tuple insert = null;
		Tuple search = null;
		Tuple read = null;

		try {
			TupleBuilder toInsert = new TupleBuilder(def);
			toInsert.addValue(key.get());
			toInsert.addValue(((VectorClock) value.getVersion()).toBytes());
			toInsert.addValue(value.getValue());

			t = this.database
					.beginTransaction(TransactionLevel.REPEATABLE_READ);
			c = t.openTable(def);

			search = c.createClusteredIndexSearchTuple(new TupleBuilder(def)
					.addValues(key.get()));
			read = c.createClusteredIndexReadTuple();

			c.find(search, SearchMode.GE);
			c.setLockMode(LockMode.LOCK_EXCLUSIVE);

			boolean locked = false;

			while (c.hasNext()) {
				c.readRow(read);

				byte[] theKey = read.getBytes(0);
				byte[] theVersionBytes = read.getBytes(1);

				if (theKey != null && theVersionBytes != null
						&& Arrays.equals(key.get(), theKey)) {
					VectorClock theVersion = new VectorClock(theVersionBytes);

					Occured occured = value.getVersion().compare(theVersion);

					if (occured == Occured.BEFORE) {
						throw new ObsoleteVersionException(
								"Attempt to put version " + value.getVersion()
										+ " which is superceeded by "
										+ theVersion + ".");
					} else if (occured == Occured.AFTER) {
						if (!locked) {
							c.lock(LockMode.INTENTION_EXCLUSIVE);
							locked = true;
						}
						c.deleteRow();
					}
				} else {
					break;
				}

				c.next();
				read.clear();
			}

			insert = c.createClusteredIndexReadTuple();
			if (!locked) {
				c.lock(LockMode.INTENTION_EXCLUSIVE);
			}
			c.insertRow(insert, toInsert);
			insert.clear();
			c.close();
			c = null;

			t.commit();
			t = null;
		} catch (ObsoleteVersionException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (search != null) {
				search.delete();
			}

			if (read != null) {
				read.delete();
			}

			if (insert != null) {
				insert.delete();
			}

			if (c != null) {
				c.close();
			}

			if (t != null) {
				t.rollback();
			}
		}
	}

	private class EntriesIterator implements
			ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

		private final Cursor c;
		private final Transaction t;
		private final Tuple row;

		public EntriesIterator(Cursor c, Transaction t) {
			this.c = c;
			this.t = t;
			this.row = c.createClusteredIndexReadTuple();
		}

		public void close() {
			c.close();
			t.commit();
			row.delete();
		}

		public boolean hasNext() {
			return c.hasNext();
		}

		public Pair<ByteArray, Versioned<byte[]>> next() {
			if (!c.hasNext()) {
				throw new PersistenceFailureException(
						"Next called on iterator, but no more items available!");
			}

			c.readRow(row);

			ByteArray key = new ByteArray(row.getBytes(0));
			VectorClock clock = new VectorClock(row.getBytes(1));
			byte[] value = row.getBytes(2);

			c.next();
			row.clear();

			return Pair.create(key, new Versioned<byte[]>(value, clock));
		}

		public void remove() {
			throw new UnsupportedOperationException("remove() not supported");
		}
	}

	private class KeysIterator implements ClosableIterator<ByteArray> {

		private final Cursor c;
		private final Transaction t;
		private final Tuple row;

		public KeysIterator(Cursor c, Transaction t) {
			this.c = c;
			this.t = t;
			this.row = c.createClusteredIndexReadTuple();
		}

		public void close() {
			c.close();
			t.commit();
			row.delete();
		}

		public boolean hasNext() {
			return c.hasNext();
		}

		public ByteArray next() {
			if (!c.hasNext()) {
				throw new PersistenceFailureException(
						"Next called on iterator, but no more items available!");
			}

			byte[] firstKey = null;
			byte[] keyBytes = null;

			while (c.hasNext()) {
				c.readRow(row);
				keyBytes = row.getBytes(0);

				if (firstKey == null) {
					firstKey = keyBytes;
				}

				row.clear();

				if (!Arrays.equals(firstKey, keyBytes)) {
					break;
				}

				c.next();
			}

			return new ByteArray(firstKey);
		}

		public void remove() {
			throw new UnsupportedOperationException("remove() not supported");
		}
	}
}
