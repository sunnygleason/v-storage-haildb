package voldemort.store.haildb2;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
import com.g414.haildb.Database;
import com.g414.haildb.InnoException;
import com.g414.haildb.TableBuilder;
import com.g414.haildb.TableDef;
import com.g414.haildb.TableType;
import com.g414.haildb.Transaction;
import com.g414.haildb.Transaction.TransactionLevel;
import com.g414.haildb.Transaction.TransactionState;
import com.g414.haildb.tpl.DatabaseTemplate;
import com.g414.haildb.tpl.DatabaseTemplate.TransactionCallback;
import com.g414.haildb.tpl.Functional;
import com.g414.haildb.tpl.Functional.Filter;
import com.g414.haildb.tpl.Functional.Mapping;
import com.g414.haildb.tpl.Functional.Mutation;
import com.g414.haildb.tpl.Functional.MutationType;
import com.g414.haildb.tpl.Functional.Reduction;
import com.g414.haildb.tpl.Functional.Target;
import com.g414.haildb.tpl.Functional.Traversal;
import com.g414.haildb.tpl.Functional.TraversalSpec;
import com.g414.hash.LongHash;
import com.g414.hash.impl.MurmurHash;
import com.google.common.collect.ImmutableMap;
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
    private final AtomicLong serial = new AtomicLong(0L);
    private final LongHash hash = new MurmurHash();

    public HailDBStorageEngine(Database database, String name) {
        this.name = name;
        this.database = database;
        this.dbt = new DatabaseTemplate(database);

        TableBuilder builder = new TableBuilder(schema + "/" + name);

        builder.addColumn("id_", ColumnType.INT, 8, ColumnAttribute.NOT_NULL);
        builder.addColumn("key_hash_", ColumnType.INT, 8,
                ColumnAttribute.NOT_NULL);
        builder.addColumn("key_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        builder.addColumn("version_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        builder.addColumn("value_", ColumnType.BLOB, 0);

        builder.addIndex("PRIMARY", "id_", 0, true, true);
        builder.addIndex("KEYHASH", "key_hash_", 0, false, false);

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
        this.database.createTable(def, TableType.DYNAMIC, 0);
    }

    public void truncate() {
        this.database.truncateTable(def);
    }

    public ClosableIterator<ByteArray> keys() {
        final Transaction t = this.database
                .beginTransaction(TransactionLevel.READ_COMMITTED);

        final AtomicReference<Object> ref = new AtomicReference<Object>();

        return new AllEntriesIterator<ByteArray>(t, def, new Filter() {
            public Boolean map(Map<String, Object> row) {
                ref.set(row.get("key_"));

                return true;
            }
        }, new Filter() {
            public Boolean map(java.util.Map<String, Object> row) {
                return ref.get() == null
                        || !Arrays.equals((byte[]) ref.get(),
                                (byte[]) row.get("key_"));
            }
        }, new Mapping<ByteArray>() {
            @Override
            public ByteArray map(Map<String, Object> row) {
                return new ByteArray((byte[]) row.get("key_"));
            }
        });
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        Transaction t = this.database
                .beginTransaction(TransactionLevel.READ_COMMITTED);

        return new AllEntriesIterator<Pair<ByteArray, Versioned<byte[]>>>(t,
                def, new Filter() {
                    public Boolean map(Map<String, Object> row) {
                        return true;
                    }
                }, null, new Mapping<Pair<ByteArray, Versioned<byte[]>>>() {
                    @Override
                    public Pair<ByteArray, Versioned<byte[]>> map(
                            Map<String, Object> row) {
                        return new Pair<ByteArray, Versioned<byte[]>>(
                                new ByteArray((byte[]) row.get("key_")),
                                new Versioned<byte[]>((byte[]) row
                                        .get("value_"), new VectorClock(
                                        (byte[]) row.get("version_"))));
                    }
                });
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

        final long keyHash = hash.getLongHashCode(key.get());
        final Map<String, Object> primary = ImmutableMap.<String, Object> of(
                "key_hash_", keyHash);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return row.get("key_hash_").equals(keyHash);
            }
        };

        final Filter filter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return row.get("key_hash_").equals(keyHash)
                        && Arrays.equals(key.get(), (byte[]) row.get("key_"))
                        && new VectorClock((byte[]) row.get("version_"))
                                .compare(maxVersion).equals(Occured.BEFORE);
            }
        };

        final AtomicBoolean deletedSomething = new AtomicBoolean();

        final Mapping<Mutation> r = new Mapping<Mutation>() {
            public Mutation map(Map<String, Object> row) {
                deletedSomething.set(true);

                return new Mutation(MutationType.DELETE, row);
            }
        };

        for (int retries = 0; retries < 20; retries++) {
            try {
                this.dbt.inTransaction(TransactionLevel.REPEATABLE_READ,
                        new TransactionCallback<Void>() {
                            @Override
                            public Void inTransaction(Transaction txn) {
                                Functional.apply(
                                        txn,
                                        dbt,
                                        new TraversalSpec(new Target(def,
                                                "KEYHASH"), primary,
                                                primaryFilter, filter), r)
                                        .traverseAll();

                                return null;
                            }
                        });

                return deletedSomething.get();
            } catch (Exception e) {
                if (e.getCause() instanceof InnoException) {
                    // ignore deadlock
                } else {
                    throw new PersistenceFailureException(e);
                }
            } finally {
                // this space intentionally left blank
            }

            try {
                Thread.sleep((retries << 6) + 10);
            } catch (Exception e) {
                // this space intentionally left blank
            }
        }

        throw new PersistenceFailureException(
                "Unable to delete after 20 retries");
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(
            Iterable<ByteArray> keys, Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);

        Map<ByteArray, List<Versioned<byte[]>>> result = StoreUtils
                .newEmptyHashMap(keys);

        final Reduction<List<Versioned<byte[]>>> reduction = new Reduction<List<Versioned<byte[]>>>() {
            @Override
            public List<Versioned<byte[]>> reduce(Map<String, Object> row,
                    List<Versioned<byte[]>> initial) {
                byte[] value = (byte[]) row.get("value_");
                VectorClock version = new VectorClock(
                        (byte[]) row.get("version_"));
                initial.add(new Versioned<byte[]>(value, version));

                return initial;
            }
        };

        try {
            for (final ByteArray key : keys) {
                final long keyHash = hash.getLongHashCode(key.get());

                final Map<String, Object> primary = ImmutableMap
                        .<String, Object> of("key_hash_", keyHash);

                final Filter primaryFilter = new Filter() {
                    public Boolean map(Map<String, Object> row) {
                        return row.get("key_hash_").equals(keyHash);
                    }
                };

                final Filter filter = new Filter() {
                    public Boolean map(Map<String, Object> row) {
                        return row.get("key_hash_").equals(keyHash)
                                && Arrays.equals(key.get(),
                                        (byte[]) row.get("key_"));
                    }
                };

                final List<Versioned<byte[]>> found = Lists.newArrayList();
                dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                        new TransactionCallback<Void>() {
                            @Override
                            public Void inTransaction(Transaction txn) {
                                Functional.reduce(txn, new TraversalSpec(
                                        new Target(def, "KEYHASH"), primary,
                                        primaryFilter, filter), reduction,
                                        found);

                                return null;
                            }
                        });

                if (found.size() > 0) {
                    result.put(key, found);
                }
            }
        } catch (Exception e) {
            throw new VoldemortException(e);
        }

        return result;
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);

        return StoreUtils.get(this, key, transforms);
    }

    public String getName() {
        return name;
    }

    public void put(final ByteArray key, final Versioned<byte[]> value,
            byte[] transformed) throws PersistenceFailureException {
        StoreUtils.assertValidKey(key);
        final long keyHash = hash.getLongHashCode(key.get());

        final Map<String, Object> primary = ImmutableMap.<String, Object> of(
                "key_hash_", keyHash);

        final Filter primaryFilter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return row.get("key_hash_").equals(keyHash);
            }
        };

        final Filter filter = new Filter() {
            public Boolean map(Map<String, Object> row) {
                return row.get("key_hash_").equals(keyHash)
                        && Arrays.equals(key.get(), (byte[]) row.get("key_"));
            }
        };

        final AtomicBoolean didUpdate = new AtomicBoolean();

        final Mapping<Mutation> mutation = new Mapping<Mutation>() {
            @Override
            public Mutation map(Map<String, Object> row) {
                VectorClock rowVersion = new VectorClock(
                        (byte[]) row.get("version_"));
                Occured occured = value.getVersion().compare(rowVersion);

                if (occured.equals(Occured.BEFORE)) {
                    throw new ObsoleteVersionException(
                            "Attempt to put version " + value.getVersion()
                                    + " which is superceeded by " + rowVersion
                                    + ".");
                } else if (occured.equals(Occured.AFTER)) {
                    if (didUpdate.get()) {
                        return new Mutation(MutationType.DELETE, row);
                    } else {
                        didUpdate.set(true);

                        Map<String, Object> newRow = new LinkedHashMap<String, Object>();
                        newRow.putAll(row);
                        newRow.put("version_", value.getVersion());
                        newRow.put("value_", value.getVersion());

                        return new Mutation(MutationType.INSERT_OR_UPDATE,
                                newRow);
                    }
                }

                return new Mutation(MutationType.NONE, null);
            }
        };

        boolean done = false;

        for (int retries = 0; !done && retries < 20; retries++) {
            try {
                dbt.inTransaction(TransactionLevel.READ_COMMITTED,
                        new TransactionCallback<Void>() {
                            @Override
                            public Void inTransaction(Transaction txn) {
                                Functional.apply(
                                        txn,
                                        dbt,
                                        new TraversalSpec(new Target(def,
                                                "KEYHASH"), primary,
                                                primaryFilter, filter),
                                        mutation).traverseAll();

                                if (!didUpdate.get()) {
                                    Map<String, Object> newRow = new LinkedHashMap<String, Object>();
                                    newRow.put("id_", serial.getAndIncrement());
                                    newRow.put("key_hash_", keyHash);
                                    newRow.put("key_", key.get());
                                    newRow.put("version_", ((VectorClock) value
                                            .getVersion()).toBytes());
                                    newRow.put("value_", value.getValue());

                                    dbt.insert(txn, def, newRow);
                                }

                                return null;
                            }
                        });

                return;
            } catch (Exception e) {
                if (e.getCause() instanceof InnoException) {
                    // ignore deadlock
                } else {
                    throw new PersistenceFailureException(e);
                }
            } finally {
                // this space intentionally left blank
            }

            try {
                Thread.sleep((retries << 6) + 10);
            } catch (Exception e) {
                // this space intentionally left blank
            }
        }

        throw new PersistenceFailureException(
                "Unable to insert after 20 retries");
    }

    private static class AllEntriesIterator<T> implements ClosableIterator<T> {
        final Traversal<T> iter;
        final Transaction txn;

        public AllEntriesIterator(Transaction txn, TableDef def,
                Filter primaryFilter, Filter filter, Mapping<T> mapping) {
            this.txn = txn;
            this.iter = Functional.map(txn, new TraversalSpec(new Target(def),
                    null, primaryFilter, filter), mapping);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public T next() {
            return iter.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            if (txn.getState().equals(TransactionState.NOT_STARTED)) {
                txn.release();
            } else {
                txn.commit();
            }
        }
    }
}
