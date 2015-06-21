package com.couchbase.lite.store;

import com.couchbase.lite.*;
import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.storage.SQLiteStorageEngineFactory;
import com.couchbase.lite.support.Base64;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.TextUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hideki on 6/16/15.
 */
public class SQLiteStore implements Store {
    public String TAG = Log.TAG_DATABASE;

    // Default value for maxRevTreeDepth, the max rev depth to preserve in a prune operation
    private static final int DEFAULT_MAX_REVS = Integer.MAX_VALUE;

    // First-time initialization:
    // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
    // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
    public static final String SCHEMA = "" +
            "CREATE TABLE docs ( " +
            "        doc_id INTEGER PRIMARY KEY, " +
            "        docid TEXT UNIQUE NOT NULL); " +
            "    CREATE INDEX docs_docid ON docs(docid); " +
            "    CREATE TABLE revs ( " +
            "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, " +
            "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, " +
            "        current BOOLEAN, " +
            "        deleted BOOLEAN DEFAULT 0, " +
            "        json BLOB, " +
            "        UNIQUE (doc_id, revid)); " +
            "    CREATE INDEX revs_current ON revs(doc_id, current); " +
            "    CREATE INDEX revs_parent ON revs(parent); " +
            "    CREATE TABLE localdocs ( " +
            "        docid TEXT UNIQUE NOT NULL, " +
            "        revid TEXT NOT NULL COLLATE REVID, " +
            "        json BLOB); " +
            "    CREATE INDEX localdocs_by_docid ON localdocs(docid); " +
            "    CREATE TABLE views ( " +
            "        view_id INTEGER PRIMARY KEY, " +
            "        name TEXT UNIQUE NOT NULL," +
            "        version TEXT, " +
            "        lastsequence INTEGER DEFAULT 0); " +
            "    CREATE INDEX views_by_name ON views(name); " +
            "    CREATE TABLE maps ( " +
            "        view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        key TEXT NOT NULL COLLATE JSON, " +
            "        value TEXT); " +
            "    CREATE INDEX maps_keys on maps(view_id, key COLLATE JSON); " +
            "    CREATE TABLE attachments ( " +
            "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
            "        filename TEXT NOT NULL, " +
            "        key BLOB NOT NULL, " +
            "        type TEXT, " +
            "        length INTEGER NOT NULL, " +
            "        revpos INTEGER DEFAULT 0); " +
            "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); " +
            "    CREATE TABLE replicators ( " +
            "        remote TEXT NOT NULL, " +
            "        push BOOLEAN, " +
            "        last_sequence TEXT, " +
            "        UNIQUE (remote, push)); " +
            "    PRAGMA user_version = 3"; // at the end, update user_version

    // transactionLevel is per thread
    static class TransactionLevel extends ThreadLocal<Integer> {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    }

    private String path;
    private Manager manager;
    private SQLiteStorageEngine storageEngine;
    private TransactionLevel transactionLevel;
    private StoreDelegate delegate;
    private int maxRevTreeDepth;

    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    public SQLiteStore(String path, Manager manager, StoreDelegate delegate) {
        assert (new File(path).isAbsolute()); // path must be absolute
        this.path = path;
        this.manager = manager;
        this.storageEngine = null;
        this.transactionLevel = new TransactionLevel();
        this.delegate = delegate;
        this.maxRevTreeDepth = DEFAULT_MAX_REVS;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of Storage
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // INITIALIZATION AND CONFIGURATION:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public synchronized boolean open() {

        // Create the storage engine.
        SQLiteStorageEngineFactory sqliteStorageEngineFactoryDefault = manager.getContext().getSQLiteStorageEngineFactory();
        storageEngine = sqliteStorageEngineFactoryDefault.createStorageEngine();

        // Try to open the storage engine and stop if we fail.
        if (storageEngine == null || !storageEngine.open(path)) {
            String msg = "Unable to create a storage engine, fatal error";
            Log.e(TAG, msg);
            throw new IllegalStateException(msg);
        }

        // Stuff we need to initialize every time the sqliteDb opens:
        if (!initialize("PRAGMA foreign_keys = ON;")) {
            Log.e(TAG, "Error turning on foreign keys");
            return false;
        }

        // Check the user_version number we last stored in the sqliteDb:
        int dbVersion = storageEngine.getVersion();

        // Incompatible version changes increment the hundreds' place:
        if (dbVersion >= 200) {
            Log.e(TAG, "Database: Database version (%d) is newer than I know how to work with", dbVersion);
            storageEngine.close();
            return false;
        }

        boolean isNew = (dbVersion == 0);
        boolean isSuccessful = false;

        // BEGIN TRANSACTION
        if (!beginTransaction()) {
            storageEngine.close();
            return false;
        }

        try {
            if (dbVersion < 1) {
                // First-time initialization:
                // (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
                // monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
                if (!initialize(SCHEMA)) {
                    return false;
                }
                dbVersion = 3;
            }

            if (dbVersion < 2) {
                // Version 2: added attachments.revpos
                String upgradeSql = "ALTER TABLE attachments ADD COLUMN revpos INTEGER DEFAULT 0; " +
                        "PRAGMA user_version = 2";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 2;
            }

            if (dbVersion < 3) {
                String upgradeSql = "CREATE TABLE IF NOT EXISTS localdocs ( " +
                        "docid TEXT UNIQUE NOT NULL, " +
                        "revid TEXT NOT NULL, " +
                        "json BLOB); " +
                        "CREATE INDEX IF NOT EXISTS localdocs_by_docid ON localdocs(docid); " +
                        "PRAGMA user_version = 3";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 3;
            }

            if (dbVersion < 4) {
                String upgradeSql = "CREATE TABLE info ( " +
                        "key TEXT PRIMARY KEY, " +
                        "value TEXT); " +
                        "INSERT INTO INFO (key, value) VALUES ('privateUUID', '" + Misc.CreateUUID() + "'); " +
                        "INSERT INTO INFO (key, value) VALUES ('publicUUID',  '" + Misc.CreateUUID() + "'); " +
                        "PRAGMA user_version = 4";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 4;
            }

            if (dbVersion < 5) {
                // Version 5: added encoding for attachments
                String upgradeSql = "ALTER TABLE attachments ADD COLUMN encoding INTEGER DEFAULT 0; " +
                        "ALTER TABLE attachments ADD COLUMN encoded_length INTEGER; " +
                        "PRAGMA user_version = 5";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 5;
            }


            if (dbVersion < 6) {
                // Version 6: enable Write-Ahead Log (WAL) <http://sqlite.org/wal.html>
                // Not supported on Android, require SQLite 3.7.0
                //String upgradeSql  = "PRAGMA journal_mode=WAL; " +

                // NOTE: For Android, WAL should be set when open the storageEngine
                //       Check AndroidSQLiteStorageEngine.java public boolean open(String path) method.
                //       http://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#enableWriteAheadLogging()

                String upgradeSql = "PRAGMA user_version = 6";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 6;
            }

            if (dbVersion < 7) {
                // Version 7: enable full-text search
                // Note: Apple's SQLite build does not support the icu or unicode61 tokenizers :(
                // OPT: Could add compress/decompress functions to make stored content smaller
                // Not supported on Android
                //String upgradeSql = "CREATE VIRTUAL TABLE fulltext USING fts4(content, tokenize=unicodesn); " +
                //"ALTER TABLE maps ADD COLUMN fulltext_id INTEGER; " +
                //"CREATE INDEX IF NOT EXISTS maps_by_fulltext ON maps(fulltext_id); " +
                //"CREATE TRIGGER del_fulltext DELETE ON maps WHEN old.fulltext_id not null " +
                //"BEGIN DELETE FROM fulltext WHERE rowid=old.fulltext_id| END; " +
                String upgradeSql = "PRAGMA user_version = 7";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 7;
            }

            // (Version 8 was an older version of the geo index)

            if (dbVersion < 9) {
                // Version 9: Add geo-query index
                //String upgradeSql = "CREATE VIRTUAL TABLE bboxes USING rtree(rowid, x0, x1, y0, y1); " +
                //"ALTER TABLE maps ADD COLUMN bbox_id INTEGER; " +
                //"ALTER TABLE maps ADD COLUMN geokey BLOB; " +
                //"CREATE TRIGGER del_bbox DELETE ON maps WHEN old.bbox_id not null " +
                //"BEGIN DELETE FROM bboxes WHERE rowid=old.bbox_id| END; " +
                String upgradeSql = "PRAGMA user_version = 9";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 9;
            }

            if (dbVersion < 10) {
                // Version 10: Add rev flag for whether it has an attachment
                String upgradeSql = "ALTER TABLE revs ADD COLUMN no_attachments BOOLEAN; " +
                        "PRAGMA user_version = 10";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 10;
            }

            // (Version 11 used to create the index revs_cur_deleted, which is obsoleted in version 16)

            // (Version 12: CBL Android/Java skipped 12 before. Instead, we ported bug fix at dbVersion 18)

            if (dbVersion < 13) {
                // Version 13: Add rows to track number of rows in the views
                String upgradeSql = "ALTER TABLE views ADD COLUMN total_docs INTEGER DEFAULT -1; " +
                        "PRAGMA user_version = 13";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 13;
            }

            if (dbVersion < 14) {
                // Version 14: Add index for getting a document with doc and rev id
                String upgradeSql = "CREATE INDEX IF NOT EXISTS revs_by_docid_revid ON revs(doc_id, revid desc, current, deleted); " +
                        "PRAGMA user_version = 14";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 14;
            }

            if (dbVersion < 15) {
                // Version 15: Add sequence index on maps and attachments for revs(sequence) on DELETE CASCADE
                String upgradeSql = "CREATE INDEX maps_sequence ON maps(sequence); " +
                        "CREATE INDEX attachments_sequence ON attachments(sequence); " +
                        "PRAGMA user_version = 15";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 15;
            }


            if (dbVersion < 16) {
                // Version 16: Fix the very suboptimal index revs_cur_deleted.
                // The new revs_current is an optimal index for finding the winning revision of a doc.
                String upgradeSql = "DROP INDEX IF EXISTS revs_current; " +
                        "DROP INDEX IF EXISTS revs_cur_deleted; " +
                        "CREATE INDEX revs_current ON revs(doc_id, current desc, deleted, revid desc);" +
                        "PRAGMA user_version = 16";

                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 16;
            }

            if (dbVersion < 17) {
                // Version 17: https://github.com/couchbase/couchbase-lite-ios/issues/615
                String upgradeSql = "CREATE INDEX maps_view_sequence ON maps(view_id, sequence); " +
                        "PRAGMA user_version = 17";

                if (!initialize(upgradeSql)) {
                    storageEngine.close();
                    return false;
                }
                dbVersion = 17;
            }

            // Note: We skipped change for dbVersion 12 before. 18 is for JSONCollator bug fix.
            //       Android version should be one version higher.
            if (dbVersion < 18) {
                // Version 12: Because of a bug fix that changes JSON collation, invalidate view indexes

                // instead of delete all rows in maps table, drop table and recreate it.
                String upgradeSql = "DROP TABLE maps";
                if (!initialize(upgradeSql)) {
                    return false;
                }

                upgradeSql = "CREATE TABLE IF NOT EXISTS maps ( " +
                        " view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
                        " sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                        " key TEXT NOT NULL COLLATE JSON, " +
                        " value TEXT); " +
                        " CREATE INDEX IF NOT EXISTS maps_keys on maps(view_id, key COLLATE JSON); " +
                        " CREATE INDEX IF NOT EXISTS maps_sequence ON maps(sequence);";
                if (!initialize(upgradeSql)) {
                    return false;
                }

                upgradeSql = "UPDATE views SET lastsequence=0; " +
                        "PRAGMA user_version = 18";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 18;
            }

            // NOTE: Following lines of code are for compatibility with Couchbase Lite iOS v1.1.0 storageEngine format.
            //       https://github.com/couchbase/couchbase-lite-java-core/issues/596
            //       CBL iOS v1.1.0 => 101
            //       1. Creates attachments table if it does not exist.
            //       2. Iterate revs table to populate attachments table.
            if (dbVersion >= 101) {

                // NOTE: CBL iOS v1.1.0 does not have maps table, Needs to create it if it does not exist.
                String upgradeSql = "CREATE TABLE IF NOT EXISTS maps ( " +
                        " view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, " +
                        " sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                        " key TEXT NOT NULL COLLATE JSON, " +
                        " value TEXT); " +
                        " CREATE INDEX IF NOT EXISTS maps_keys on maps(view_id, key COLLATE JSON); " +
                        " CREATE INDEX IF NOT EXISTS maps_sequence ON maps(sequence);";
                if (!initialize(upgradeSql)) {
                    return false;
                }


                // Check if attachments table exists. If not, create the table, and iterate revs
                // to populate attachment table
                boolean existsAttachments = false;
                Cursor cursor = null;
                try {
                    cursor = storageEngine.rawQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='attachments'", null);
                    if (cursor.moveToNext()) {
                        existsAttachments = true;
                    }
                } catch (SQLException e) {
                    Log.e(TAG, "Failed to check if attachments table exists", e);
                    return false;
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }

                if (!existsAttachments) {
                    // 1. create attachments table
                    upgradeSql = "CREATE TABLE attachments ( " +
                            "sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, " +
                            "filename TEXT NOT NULL, " +
                            "key BLOB NOT NULL, " +
                            "type TEXT, " +
                            "length INTEGER NOT NULL, " +
                            "encoding INTEGER DEFAULT 0, " +
                            "encoded_length INTEGER, " +
                            "revpos INTEGER DEFAULT 0); " +
                            "CREATE INDEX attachments_by_sequence on attachments(sequence, filename); " +
                            "CREATE INDEX attachments_sequence ON attachments(sequence); " +
                            "PRAGMA user_version = 20";
                    if (!initialize(upgradeSql)) {
                        return false;
                    }

                    // 2. iterate revs table, and populate attachment table
                    String sql = "SELECT sequence, json FROM revs WHERE no_attachments=0";
                    Cursor cursor2 = null;
                    try {
                        cursor2 = storageEngine.rawQuery(sql, null);
                        while (cursor2.moveToNext()) {
                            if (!cursor2.isNull(1)) {
                                long sequence = cursor2.getLong(0);
                                byte[] json = cursor2.getBlob(1);
                                try {
                                    Map<String, Object> docProperties = Manager.getObjectMapper().readValue(json, Map.class);
                                    Map<String, Object> attachments = (Map<String, Object>) docProperties.get("_attachments");
                                    Iterator<String> itr = attachments.keySet().iterator();
                                    while (itr.hasNext()) {
                                        String name = itr.next();
                                        Map<String, Object> attachment = (Map<String, Object>) attachments.get(name);
                                        String contentType = (String) attachment.get("content_type");
                                        int revPos = (Integer) attachment.get("revpos");
                                        int length = (Integer) attachment.get("length");
                                        String digest = (String) attachment.get("digest");
                                        int encodedLength = -1;
                                        if (attachment.containsKey("encoded_length"))
                                            encodedLength = (Integer) attachment.get("encoded_length");
                                        AttachmentInternal.AttachmentEncoding encoding = AttachmentInternal.AttachmentEncoding.AttachmentEncodingNone;
                                        if (attachment.containsKey("encoding") && attachment.get("encoding").equals("gzip")) {
                                            encoding = AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP;
                                        }
                                        BlobKey key = new BlobKey(digest);
                                        try {
                                            insertAttachmentForSequenceWithNameAndType(sequence, name, contentType, revPos, key, length, encoding, encodedLength);
                                        } catch (CouchbaseLiteException e) {
                                            Log.e(Log.TAG_DATABASE, "Attachment information inserstion error: " + name + "=" + attachment.toString(), e);
                                            return false;
                                        }
                                    }
                                } catch (Exception e) {
                                    Log.e(Log.TAG_DATABASE, "JSON parsing error: " + new String(json), e);
                                    return false;
                                }
                            }
                        }
                    } catch (SQLException e) {
                        Log.e(TAG, "Failed to check if attachments table exists", e);
                        return false;
                    } finally {
                        if (cursor2 != null) {
                            cursor2.close();
                        }
                    }
                }
                dbVersion = 20;
            }

            // NOTE: CBL Android/Java v1.1.0 Set storageEngine version 20.
            //       20 is higher than any previous release, but lower than CBL iOS v1.1.0 - 101
            if (dbVersion < 20) {
                String upgradeSql = "PRAGMA user_version = 20";
                if (!initialize(upgradeSql)) {
                    return false;
                }
                dbVersion = 20;
            }

            if (isNew) {
                optimizeSQLIndexes(); // runs ANALYZE query
            }

            // successfully updated storageEngine schema
            isSuccessful = true;

        } finally {
            // END TRANSACTION WITH COMMIT OR ROLLBACK
            endTransaction(isSuccessful);

            // if failed, close storageEngine before return
            if (!isSuccessful) {
                storageEngine.close();
            }
        }

        return true;
    }

    @Override
    public void close() {
        if (storageEngine != null && storageEngine.isOpen())
            storageEngine.close();
        storageEngine = null;
    }

    @Override
    public void setDelegate(StoreDelegate delegate) {
        this.delegate = delegate;
    }

    @Override
    public StoreDelegate getDelegate() {
        return delegate;
    }

    /**
     * Set the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @Override
    public void setMaxRevTreeDepth(int maxRevTreeDepth) {
        this.maxRevTreeDepth = maxRevTreeDepth;
    }

    /**
     * Get the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    @Override
    public int getMaxRevTreeDepth() {
        return maxRevTreeDepth;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DATABASE ATTRIBUTES & OPERATIONS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public long setInfo(String key, String info) {
        long result = 0;
        try {
            ContentValues args = new ContentValues();
            args.put("key", key);
            args.put("value", info);
            result = storageEngine.insertWithOnConflict("info", null, args, SQLiteStorageEngine.CONFLICT_REPLACE);

        } catch (Exception e) {
            Log.e(TAG, "Error inserting document id", e);
        }
        return result;
    }

    @Override
    public String getInfo(String key) {
        String result = null;
        Cursor cursor = null;
        try {
            String[] args = {key};
            cursor = storageEngine.rawQuery("SELECT value FROM info WHERE key=?", args);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error querying " + key, e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public int getDocumentCount() {
        String sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
        Cursor cursor = null;
        int result = 0;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            if (cursor.moveToNext()) {
                result = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting document count", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * The latest sequence number used.  Every new revision is assigned a new sequence number,
     * so this property increases monotonically as changes are made to the storageEngine. It can be
     * used to check whether the storageEngine has changed between two points in time.
     */
    public long getLastSequence() {
        String sql = "SELECT MAX(sequence) FROM revs";
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting last sequence", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Is a transaction active?
     */
    @Override
    public boolean inTransaction() {
        return transactionLevel.get() > 0;
    }

    @Override
    public void compact() throws CouchbaseLiteException {
        // Can't delete any rows because that would lose revision tree history.
        // But we can remove the JSON of non-current revisions, which is most of the space.
        try {
            Log.v(TAG, "Pruning old revisions...");
            pruneRevsToMaxDepth(0);
            Log.v(TAG, "Deleting JSON of old revisions...");
            ContentValues args = new ContentValues();
            args.put("json", (String) null);
            storageEngine.update("revs", args, "current=0", null);
        } catch (SQLException e) {
            Log.e(TAG, "Error compacting", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }

        Log.v(TAG, "Deleting old attachments...");
        Status result = garbageCollectAttachments();
        if (!result.isSuccessful()) {
            throw new CouchbaseLiteException(result);
        }

        Log.v(TAG, "Vacuuming SQLite sqliteDb...");
        try {
            storageEngine.execSQL("VACUUM");
        } catch (SQLException e) {
            Log.e(TAG, "Error vacuuming sqliteDb", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public boolean runInTransaction(TransactionalTask transactionalTask) {

        boolean shouldCommit = true;

        beginTransaction();
        try {
            shouldCommit = transactionalTask.run();
        } catch (Exception e) {
            shouldCommit = false;
            Log.e(TAG, e.toString(), e);
            throw new RuntimeException(e);
        } finally {
            endTransaction(shouldCommit);
        }

        return shouldCommit;
    }

    ///////////////////////////////////////////////////////////////////////////
    // DOCUMENTS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public RevisionInternal getDocument(String docID, String revID, EnumSet<Database.TDContentOptions> contentOptions) {
        RevisionInternal result = null;
        String sql;

        Cursor cursor = null;
        try {
            cursor = null;
            String cols = "revid, deleted, sequence, no_attachments";
            if (!contentOptions.contains(Database.TDContentOptions.TDNoBody)) {
                cols += ", json";
            }
            if (revID != null) {
                sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1";
                //TODO: mismatch w iOS: {sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? AND revid=? AND json notnull LIMIT 1";}
                String[] args = {docID, revID};
                cursor = storageEngine.rawQuery(sql, args);
            } else {
                sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";
                //TODO: mismatch w iOS: {sql = "SELECT " + cols + " FROM revs WHERE revs.doc_id=? and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";}
                String[] args = {docID};
                cursor = storageEngine.rawQuery(sql, args);
            }

            if (cursor.moveToNext()) {
                if (revID == null) {
                    revID = cursor.getString(0);
                }
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(docID, revID, deleted);
                result.setSequence(cursor.getLong(2));
                if (!contentOptions.equals(EnumSet.of(Database.TDContentOptions.TDNoBody))) {
                    byte[] json = null;
                    if (!contentOptions.contains(Database.TDContentOptions.TDNoBody)) {
                        json = cursor.getBlob(4);
                    }
                    if (cursor.getInt(3) > 0) // no_attachments == true
                        contentOptions.add(Database.TDContentOptions.TDNoAttachments);
                    expandStoredJSONIntoRevisionWithAttachments(json, result, contentOptions);
                }
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting document with id and rev", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public RevisionInternal loadRevisionBody(RevisionInternal rev, EnumSet<Database.TDContentOptions> contentOptions) throws CouchbaseLiteException {
        if (rev.getBody() != null && contentOptions == EnumSet.noneOf(Database.TDContentOptions.class) && rev.getSequence() != 0) {
            return rev;
        }

        if ((rev.getDocId() == null) || (rev.getRevId() == null)) {
            Log.e(TAG, "Error loading revision body");
            throw new CouchbaseLiteException(Status.DUPLICATE);
        }

        Cursor cursor = null;
        Status result = new Status(Status.NOT_FOUND);
        try {
            // TODO: on ios this query is:
            // TODO: "SELECT sequence, json FROM revs WHERE doc_id=? AND revid=? LIMIT 1"
            String sql = "SELECT sequence, json FROM revs, docs WHERE revid=? AND docs.docid=? AND revs.doc_id=docs.doc_id LIMIT 1";
            String[] args = {rev.getRevId(), rev.getDocId()};
            cursor = storageEngine.rawQuery(sql, args);
            if (cursor.moveToNext()) {
                result.setCode(Status.OK);
                rev.setSequence(cursor.getLong(0));
                expandStoredJSONIntoRevisionWithAttachments(cursor.getBlob(1), rev, contentOptions);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error loading revision body", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (result.getCode() == Status.NOT_FOUND) {
            throw new CouchbaseLiteException(result);
        }

        return rev;
    }

    @Override
    public RevisionInternal getParentRevision(RevisionInternal rev) {

        // First get the parent's sequence:
        long seq = rev.getSequence();
        if (seq > 0) {
            seq = longForQuery("SELECT parent FROM revs WHERE sequence=?", new String[]{Long.toString(seq)});
        } else {
            long docNumericID = getDocNumericID(rev.getDocId());
            if (docNumericID <= 0) {
                return null;
            }
            String[] args = new String[]{Long.toString(docNumericID), rev.getRevId()};
            seq = longForQuery("SELECT parent FROM revs WHERE doc_id=? and revid=?", args);
        }

        if (seq == 0) {
            return null;
        }

        // Now get its revID and deletion status:
        RevisionInternal result = null;

        String[] args = {Long.toString(seq)};
        String queryString = "SELECT revid, deleted FROM revs WHERE sequence=?";
        Cursor cursor = null;

        try {
            cursor = storageEngine.rawQuery(queryString, args);
            if (cursor.moveToNext()) {
                String revId = cursor.getString(0);
                boolean deleted = (cursor.getInt(1) > 0);
                result = new RevisionInternal(rev.getDocId(), revId, deleted/*, this*/);
                result.setSequence(seq);
            }
        } finally {
            cursor.close();
        }
        return result;
    }

    /**
     * Returns an array of TDRevs in reverse chronological order, starting with the given revision.
     */
    @Override
    public List<RevisionInternal> getRevisionHistory(RevisionInternal rev) {
        String docId = rev.getDocId();
        String revId = rev.getRevId();
        assert ((docId != null) && (revId != null));

        long docNumericId = getDocNumericID(docId);
        if (docNumericId < 0) {
            return null;
        } else if (docNumericId == 0) {
            return new ArrayList<RevisionInternal>();
        }

        String sql = "SELECT sequence, parent, revid, deleted, json isnull FROM revs " +
                "WHERE doc_id=? ORDER BY sequence DESC";
        String[] args = {Long.toString(docNumericId)};
        Cursor cursor = null;

        List<RevisionInternal> result;
        try {
            cursor = storageEngine.rawQuery(sql, args);

            cursor.moveToNext();
            long lastSequence = 0;
            result = new ArrayList<RevisionInternal>();
            while (!cursor.isAfterLast()) {
                long sequence = cursor.getLong(0);
                boolean matches = false;
                if (lastSequence == 0) {
                    matches = revId.equals(cursor.getString(2));
                } else {
                    matches = (sequence == lastSequence);
                }
                if (matches) {
                    revId = cursor.getString(2);
                    boolean deleted = (cursor.getInt(3) > 0);
                    boolean missing = (cursor.getInt(4) > 0);
                    RevisionInternal aRev = new RevisionInternal(docId, revId, deleted);
                    aRev.setMissing(missing);
                    aRev.setSequence(cursor.getLong(0));
                    result.add(aRev);
                    lastSequence = cursor.getLong(1);
                    if (lastSequence == 0) {
                        break;
                    }
                }
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting revision history", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    @Override
    public RevisionList getAllRevisions(String docId, long docNumericID, boolean onlyCurrent) {

        String sql = null;
        if (onlyCurrent) {
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? AND current ORDER BY sequence DESC";
        } else {
            sql = "SELECT sequence, revid, deleted FROM revs " +
                    "WHERE doc_id=? ORDER BY sequence DESC";
        }

        String[] args = {Long.toString(docNumericID)};
        Cursor cursor = null;

        cursor = storageEngine.rawQuery(sql, args);

        RevisionList result;
        try {
            cursor.moveToNext();
            result = new RevisionList();
            while (!cursor.isAfterLast()) {
                RevisionInternal rev = new RevisionInternal(docId, cursor.getString(1), (cursor.getInt(2) > 0));
                rev.setSequence(cursor.getLong(0));
                result.add(rev);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting all revisions of document", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    @Override
    public List<String> getPossibleAncestorRevisionIDs(RevisionInternal rev, int limit, AtomicBoolean onlyAttachments) {
        int generation = rev.getGeneration();
        if (generation <= 1)
            return null;

        long docNumericID = getDocNumericID(rev.getDocId());
        if (docNumericID <= 0)
            return null;

        List<String> revIDs = new ArrayList<String>();

        int sqlLimit = limit > 0 ? (int) limit : -1;     // SQL uses -1, not 0, to denote 'no limit'
        String sql = "SELECT revid, sequence FROM revs WHERE doc_id=? and revid < ?" +
                " and deleted=0 and json not null" +
                " ORDER BY sequence DESC LIMIT ?";
        String[] args = {Long.toString(docNumericID), generation + "-", Integer.toString(sqlLimit)};

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                if (onlyAttachments != null && revIDs.size() == 0) {
                    onlyAttachments.set(sequenceHasAttachments(cursor.getLong(1)));
                }
                revIDs.add(cursor.getString(0));
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return revIDs;
    }

    @Override
    public String findCommonAncestor(RevisionInternal rev, List<String> revIDs) {
        String result = null;

        if (revIDs.size() == 0)
            return null;
        String docId = rev.getDocId();
        long docNumericID = getDocNumericID(docId);
        if (docNumericID <= 0)
            return null;
        String quotedRevIds = TextUtils.joinQuoted(revIDs);
        String sql = "SELECT revid FROM revs " +
                "WHERE doc_id=? and revid in (" + quotedRevIds + ") and revid <= ? " +
                "ORDER BY revid DESC LIMIT 1";
        String[] args = {Long.toString(docNumericID)};

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            if (!cursor.isAfterLast()) {
                result = cursor.getString(0);
            }

        } catch (SQLException e) {
            Log.e(TAG, "Error getting all revisions of document", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    @Override
    public int findMissingRevisions(RevisionList touchRevs) throws SQLException {
        int numRevisionsRemoved = 0;
        if (touchRevs.size() == 0) {
            return numRevisionsRemoved;
        }

        String quotedDocIds = TextUtils.joinQuoted(touchRevs.getAllDocIds());
        String quotedRevIds = TextUtils.joinQuoted(touchRevs.getAllRevIds());

        String sql = "SELECT docid, revid FROM revs, docs " +
                "WHERE docid IN (" +
                quotedDocIds +
                ") AND revid in (" +
                quotedRevIds + ")" +
                " AND revs.doc_id == docs.doc_id";

        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(sql, null);
            cursor.moveToNext();
            while (!cursor.isAfterLast()) {
                RevisionInternal rev = touchRevs.revWithDocIdAndRevId(cursor.getString(0), cursor.getString(1));

                if (rev != null) {
                    touchRevs.remove(rev);
                    numRevisionsRemoved += 1;
                }

                cursor.moveToNext();
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return numRevisionsRemoved;
    }

    @Override
    public Map<String, Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException {

        Map<String, Object> result = new HashMap<String, Object>();
        List<QueryRow> rows = new ArrayList<QueryRow>();
        if (options == null) {
            options = new QueryOptions();
        }
        boolean includeDeletedDocs = (options.getAllDocsMode() == Query.AllDocsMode.INCLUDE_DELETED);

        long updateSeq = 0;
        if (options.isUpdateSeq()) {
            updateSeq = getLastSequence();  // TODO: needs to be atomic with the following SELECT
        }

        StringBuffer sql = new StringBuffer("SELECT revs.doc_id, docid, revid, sequence");
        if (options.isIncludeDocs()) {
            sql.append(", json");
        }
        if (includeDeletedDocs) {
            sql.append(", deleted");
        }
        sql.append(" FROM revs, docs WHERE");
        if (options.getKeys() != null) {
            if (options.getKeys().size() == 0) {
                return result;
            }
            String commaSeperatedIds = TextUtils.joinQuotedObjects(options.getKeys());
            sql.append(String.format(" revs.doc_id IN (SELECT doc_id FROM docs WHERE docid IN (%s)) AND", commaSeperatedIds));
        }
        sql.append(" docs.doc_id = revs.doc_id AND current=1");
        if (!includeDeletedDocs) {
            sql.append(" AND deleted=0");
        }
        List<String> args = new ArrayList<String>();
        Object minKey = options.getStartKey();
        Object maxKey = options.getEndKey();
        boolean inclusiveMin = true;
        boolean inclusiveMax = options.isInclusiveEnd();
        if (options.isDescending()) {
            minKey = maxKey;
            maxKey = options.getStartKey();
            inclusiveMin = inclusiveMax;
            inclusiveMax = true;
        }
        if (minKey != null) {
            assert (minKey instanceof String);
            sql.append((inclusiveMin ? " AND docid >= ?" : " AND docid > ?"));
            args.add((String) minKey);
        }
        if (maxKey != null) {
            assert (maxKey instanceof String);
            maxKey = View.keyForPrefixMatch(maxKey, options.getPrefixMatchLevel());
            sql.append((inclusiveMax ? " AND docid <= ?" : " AND docid < ?"));
            args.add((String) maxKey);
        }

        sql.append(
                String.format(
                        " ORDER BY docid %s, %s revid DESC LIMIT ? OFFSET ?",
                        (options.isDescending() ? "DESC" : "ASC"),
                        (includeDeletedDocs ? "deleted ASC," : "")
                )
        );

        args.add(Integer.toString(options.getLimit()));
        args.add(Integer.toString(options.getSkip()));

        Cursor cursor = null;
        Map<String, QueryRow> docs = new HashMap<String, QueryRow>();


        try {
            cursor = storageEngine.rawQuery(sql.toString(), args.toArray(new String[args.size()]));

            boolean keepGoing = cursor.moveToNext();

            while (keepGoing) {

                long docNumericID = cursor.getLong(0);
                String docId = cursor.getString(1);
                String revId = cursor.getString(2);
                long sequenceNumber = cursor.getLong(3);
                boolean deleted = includeDeletedDocs && cursor.getInt(getDeletedColumnIndex(options)) > 0;
                Map<String, Object> docContents = null;
                if (options.isIncludeDocs()) {
                    byte[] json = cursor.getBlob(4);
                    docContents = documentPropertiesFromJSON(json, docId, revId, deleted, sequenceNumber, options.getContentOptions());
                }

                // Iterate over following rows with the same doc_id -- these are conflicts.
                // Skip them, but collect their revIDs if the 'conflicts' option is set:
                List<String> conflicts = new ArrayList<String>();
                while (((keepGoing = cursor.moveToNext()) == true) && cursor.getLong(0) == docNumericID) {
                    if (options.getAllDocsMode() == Query.AllDocsMode.SHOW_CONFLICTS || options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS) {
                        if (conflicts.isEmpty()) {
                            conflicts.add(revId);
                        }
                        conflicts.add(cursor.getString(2));
                    }
                }

                if (options.getAllDocsMode() == Query.AllDocsMode.ONLY_CONFLICTS && conflicts.isEmpty()) {
                    continue;
                }

                Map<String, Object> value = new HashMap<String, Object>();
                value.put("rev", revId);
                value.put("_conflicts", conflicts);
                if (includeDeletedDocs) {
                    value.put("deleted", (deleted ? true : null));
                }
                QueryRow change = new QueryRow(docId, sequenceNumber, docId, value, docContents, null);
                if (options.getKeys() != null) {
                    docs.put(docId, change);
                }
                // TODO: In the future, we need to implement CBLRowPassesFilter() in CBLView+Querying.m
                else if (options.getPostFilter() == null || options.getPostFilter().apply(change)) {
                    rows.add(change);
                }
            }

            if (options.getKeys() != null) {
                for (Object docIdObject : options.getKeys()) {
                    if (docIdObject instanceof String) {
                        String docId = (String) docIdObject;
                        QueryRow change = docs.get(docId);
                        if (change == null) {
                            Map<String, Object> value = new HashMap<String, Object>();
                            long docNumericID = getDocNumericID(docId);
                            if (docNumericID > 0) {
                                boolean deleted;
                                AtomicBoolean outIsDeleted = new AtomicBoolean(false);
                                AtomicBoolean outIsConflict = new AtomicBoolean();
                                String revId = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
                                if (outIsDeleted.get()) {
                                    deleted = true;
                                }
                                if (revId != null) {
                                    value.put("rev", revId);
                                    value.put("deleted", true);
                                }
                            }
                            change = new QueryRow((value != null ? docId : null), 0, docId, value, null, null);
                        }
                        rows.add(change);
                    }
                }
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting all docs", e);
            throw new CouchbaseLiteException("Error getting all docs", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        result.put("rows", rows);
        result.put("total_rows", rows.size());
        result.put("offset", options.getSkip());
        if (updateSeq != 0) {
            result.put("update_seq", updateSeq);
        }

        return result;
    }

    @Override
    public RevisionList changesSince(long lastSequence, ChangesOptions options, ReplicationFilter filter, Map<String, Object> filterParams) {
        // http://wiki.apache.org/couchdb/HTTP_database_API#Changes
        if (options == null) {
            options = new ChangesOptions();
        }

        boolean includeDocs = options.isIncludeDocs() || (filter != null);
        String additionalSelectColumns = "";
        if (includeDocs) {
            additionalSelectColumns = ", json";
        }

        String sql = "SELECT sequence, revs.doc_id, docid, revid, deleted" + additionalSelectColumns + " FROM revs, docs "
                + "WHERE sequence > ? AND current=1 "
                + "AND revs.doc_id = docs.doc_id "
                + "ORDER BY revs.doc_id, revid DESC";
        String[] args = {Long.toString(lastSequence)};
        Cursor cursor = null;
        RevisionList changes = null;

        try {
            cursor = storageEngine.rawQuery(sql, args);
            cursor.moveToNext();
            changes = new RevisionList();
            long lastDocId = 0;
            while (!cursor.isAfterLast()) {
                if (!options.isIncludeConflicts()) {
                    // Only count the first rev for a given doc (the rest will be losing conflicts):
                    long docNumericId = cursor.getLong(1);
                    if (docNumericId == lastDocId) {
                        cursor.moveToNext();
                        continue;
                    }
                    lastDocId = docNumericId;
                }

                RevisionInternal rev = new RevisionInternal(cursor.getString(2), cursor.getString(3), (cursor.getInt(4) > 0));
                rev.setSequence(cursor.getLong(0));
                if (includeDocs) {
                    expandStoredJSONIntoRevisionWithAttachments(cursor.getBlob(5), rev, options.getContentOptions());
                }
                if (delegate.runFilter(filter, filterParams, rev)) {
                    changes.add(rev);
                }
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error looking for changes", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (options.isSortBySequence()) {
            changes.sortBySequence();
        }
        changes.limit(options.getLimit());
        return changes;
    }

    ///////////////////////////////////////////////////////////////////////////
    // INSERTION / DELETION:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Stores a new (or initial) revision of a document.
     * <p/>
     * This is what's invoked by a PUT or POST. As with those, the previous revision ID must be supplied when necessary and the call will fail if it doesn't match.
     *
     * @param oldRev        The revision to add. If the docID is null, a new UUID will be assigned. Its revID must be null. It must have a JSON body.
     * @param prevRevId     The ID of the revision to replace (same as the "?rev=" parameter to a PUT), or null if this is a new document.
     * @param allowConflict If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
     * @param resultStatus  On return, an HTTP status code indicating success or failure.
     * @return A new RevisionInternal with the docID, revID and sequence filled in (but no body).
     * <p/>
     * NOTE: Called by Internal and Unit Tests
     */
    @Override
    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal oldRev, String prevRevId, boolean allowConflict, Status resultStatus) throws CouchbaseLiteException {
        // prevRevId is the rev ID being replaced, or nil if an insert
        String docId = oldRev.getDocId();
        boolean deleted = oldRev.isDeleted();
        if ((oldRev == null) || ((prevRevId != null) && (docId == null)) || (deleted && (docId == null))
                || ((docId != null) && !Document.isValidDocumentId(docId))) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        beginTransaction();
        Cursor cursor = null;
        boolean inConflict = false;
        RevisionInternal winningRev = null;
        RevisionInternal newRev = null;

        //// PART I: In which are performed lookups and validations prior to the insert...

        long docNumericID = (docId != null) ? getDocNumericID(docId) : 0;
        long parentSequence = 0;
        AtomicBoolean oldWinnerWasDeletion = new AtomicBoolean(false);
        AtomicBoolean wasConflicted = new AtomicBoolean(false);
        String oldWinningRevID = null;

        try {
            if (docNumericID > 0) {
                try {
                    oldWinningRevID = winningRevIDOfDoc(docNumericID, oldWinnerWasDeletion, wasConflicted);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (prevRevId != null) {
                // Replacing: make sure given prevRevID is current & find its sequence number:
                if (docNumericID <= 0) {
                    String msg = String.format("No existing revision found with doc id: %s", docId);
                    throw new CouchbaseLiteException(msg, Status.NOT_FOUND);
                }

                parentSequence = getSequenceOfDocument(docNumericID, prevRevId, !allowConflict);

                if (parentSequence == 0) {
                    // Not found: either a 404 or a 409, depending on whether there is any current revision
                    if (!allowConflict && existsDocumentWithIDAndRev(docId, null)) {
                        String msg = String.format("Conflicts not allowed and there is already an existing doc with id: %s", docId);
                        throw new CouchbaseLiteException(msg, Status.CONFLICT);
                    } else {
                        String msg = String.format("No existing revision found with doc id: %s", docId);
                        throw new CouchbaseLiteException(msg, Status.NOT_FOUND);
                    }
                }

                if (delegate.getValidations() != null && delegate.getValidations().size() > 0) {
                    // Fetch the previous revision and validate the new one against it:
                    RevisionInternal fakeNewRev = oldRev.copyWithDocID(oldRev.getDocId(), null);
                    RevisionInternal prevRev = new RevisionInternal(docId, prevRevId, false);
                    delegate.validateRevision(fakeNewRev, prevRev, prevRevId);
                }

            } else {
                // Inserting first revision.
                if (deleted && (docId != null)) {
                    // Didn't specify a revision to delete: 404 or a 409, depending
                    if (existsDocumentWithIDAndRev(docId, null)) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    } else {
                        throw new CouchbaseLiteException(Status.NOT_FOUND);
                    }
                }

                // Validate:
                delegate.validateRevision(oldRev, null, null);

                if (docId != null) {
                    // Inserting first revision, with docID given (PUT):
                    if (docNumericID <= 0) {
                        // Doc doesn't exist at all; create it:
                        docNumericID = insertDocumentID(docId);
                        if (docNumericID <= 0) {
                            return null;
                        }
                    } else {

                        // Doc ID exists; check whether current winning revision is deleted:
                        if (oldWinnerWasDeletion.get() == true) {
                            prevRevId = oldWinningRevID;
                            parentSequence = getSequenceOfDocument(docNumericID, prevRevId, false);

                        } else if (oldWinningRevID != null) {
                            String msg = "The current winning revision is not deleted, so this is a conflict";
                            throw new CouchbaseLiteException(msg, Status.CONFLICT);
                        }
                    }
                } else {
                    // Inserting first revision, with no docID given (POST): generate a unique docID:
                    docId = Database.generateDocumentId();
                    docNumericID = insertDocumentID(docId);
                    if (docNumericID <= 0) {
                        return null;
                    }
                }
            }

            // There may be a conflict if (a) the document was already in conflict, or
            // (b) a conflict is created by adding a non-deletion child of a non-winning rev.
            inConflict = wasConflicted.get() ||
                    (!deleted &&
                            prevRevId != null &&
                            oldWinningRevID != null &&
                            !prevRevId.equals(oldWinningRevID));

            //// PART II: In which we prepare for insertion...

            // Get the attachments:
            Map<String, AttachmentInternal> attachments = delegate.getAttachmentsFromRevision(oldRev, prevRevId);

            // Bump the revID and update the JSON:
            byte[] json = null;
            if (oldRev.getProperties() != null && oldRev.getProperties().size() > 0) {
                json = RevisionUtils.encodeDocumentJSON(oldRev);
                if (json == null) {
                    String msg = "Bad or missing JSON";
                    throw new CouchbaseLiteException(msg, Status.BAD_REQUEST);
                }

                if (json.length == 2 && json[0] == '{' && json[1] == '}') {
                    json = null;
                }
            }

            String newRevId = RevisionUtils.generateIDForRevision(oldRev, json, attachments, prevRevId);
            newRev = oldRev.copyWithDocID(docId, newRevId);
            delegate.stubOutAttachmentsInRevision(attachments, newRev);

            // Don't store a SQL null in the 'json' column -- I reserve it to mean that the revision data
            // is missing due to compaction or replication.
            // Instead, store an empty zero-length blob.
            if (json == null)
                json = new byte[0];

            //// PART III: In which the actual insertion finally takes place:
            // Now insert the rev itself:
            long newSequence = insertRevision(newRev, docNumericID, parentSequence, true, (attachments != null), json);
            if (newSequence <= 0) {
                // The insert failed. If it was due to a constraint violation, that means a revision
                // already exists with identical contents and the same parent rev. We can ignore this
                // insert call, then.
                // TODO - figure out storageEngine error code
                // NOTE - In AndroidSQLiteStorageEngine.java, CBL Android uses insert() method of SQLiteDatabase
                //        which return -1 for error case. Instead of insert(), should use insertOrThrow method()
                //        which throws detailed error code. Need to check with SQLiteDatabase.CONFLICT_FAIL.
                //        However, returning after updating parentSequence might not cause any problem.
                //if (_fmdb.lastErrorCode != SQLITE_CONSTRAINT)
                //    return null;
                Log.w(TAG, "Duplicate rev insertion: " + docId + " / " + newRevId);
                newRev.setBody(null);
                // don't return yet; update the parent's current just to be sure (see #316 (iOS #509))
            }

            // Make replaced rev non-current:
            try {
                ContentValues args = new ContentValues();
                args.put("current", 0);
                storageEngine.update("revs", args, "sequence=?", new String[]{String.valueOf(parentSequence)});
            } catch (SQLException e) {
                Log.e(TAG, "Error setting parent rev non-current", e);
                throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
            }

            if (newSequence <= 0) {
                // duplicate rev; see above
                resultStatus.setCode(Status.OK);
                delegate.databaseStorageChanged(new DocumentChange(newRev, winningRev, inConflict, null));
                return newRev;
            }

            // Store any attachments:
            if (attachments != null) {
                delegate.processAttachmentsForRevision(attachments, newRev, parentSequence);
            }


            // Figure out what the new winning rev ID is:
            winningRev = delegate.winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion.get(), newRev);

            // Success!
            if (deleted) {
                resultStatus.setCode(Status.OK);
            } else {
                resultStatus.setCode(Status.CREATED);
            }

        } catch (SQLException e1) {
            Log.e(TAG, "Error putting revision", e1);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            endTransaction(resultStatus.isSuccessful());
        }

        //// EPILOGUE: A change notification is sent...
        delegate.databaseStorageChanged(new DocumentChange(newRev, winningRev, inConflict, null));
        return newRev;
    }

    /**
     * Inserts an already-existing revision replicated from a remote sqliteDb.
     * <p/>
     * It must already have a revision ID. This may create a conflict! The revision's history must be given; ancestor revision IDs that don't already exist locally will create phantom revisions with no content.
     *
     * @exclude in CBLDatabase+Insertion.m
     * - (CBLStatus) forceInsert: (CBL_Revision*)inRev
     * revisionHistory: (NSArray*)history  // in *reverse* order, starting with rev's revID
     * source: (NSURL*)source
     */
    @Override
    @InterfaceAudience.Private
    public void forceInsert(RevisionInternal inRev, List<String> history, URL source) throws CouchbaseLiteException {

        // TODO: in the iOS version, it is passed an immutable RevisionInternal and then
        // TODO: creates a mutable copy.  We should do the same here.
        // TODO: see github.com/couchbase/couchbase-lite-java-core/issues/206#issuecomment-44364624

        RevisionInternal winningRev = null;
        boolean inConflict = false;

        String docId = inRev.getDocId();
        String revId = inRev.getRevId();
        if (!Document.isValidDocumentId(docId) || (revId == null)) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        int historyCount = 0;
        if (history != null) {
            historyCount = history.size();
        }
        if (historyCount == 0) {
            history = new ArrayList<String>();
            history.add(revId);
            historyCount = 1;
        } else if (!history.get(0).equals(inRev.getRevId())) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        boolean success = false;
        beginTransaction();
        try {
            // First look up the document's row-id and all locally-known revisions of it:
            Map<String, RevisionInternal> localRevs = null;
            boolean isNewDoc = (historyCount == 1);
            long docNumericID = getOrInsertDocNumericID(docId);

            if (!isNewDoc) {
                RevisionList localRevsList = getAllRevisions(docId, docNumericID, false);
                if (localRevsList == null) {
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
                localRevs = new HashMap<String, RevisionInternal>();
                for (RevisionInternal r : localRevsList) {
                    localRevs.put(r.getRevId(), r);
                }
            }

            // Validate against the latest common ancestor:
            if (delegate.getValidations() != null && delegate.getValidations().size() > 0) {
                RevisionInternal oldRev = null;
                for (int i = 1; i < historyCount; i++) {
                    oldRev = (localRevs != null) ? localRevs.get(history.get(i)) : null;
                    if (oldRev != null) {
                        break;
                    }
                }
                String parentRevId = (historyCount > 1) ? history.get(1) : null;
                delegate.validateRevision(inRev, oldRev, parentRevId);
            }

            AtomicBoolean outIsDeleted = new AtomicBoolean(false);
            AtomicBoolean outIsConflict = new AtomicBoolean(false);
            boolean oldWinnerWasDeletion = false;
            String oldWinningRevID = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
            if (outIsDeleted.get()) {
                oldWinnerWasDeletion = true;
            }
            if (outIsConflict.get()) {
                inConflict = true;
            }

            // Walk through the remote history in chronological order, matching each revision ID to
            // a local revision. When the list diverges, start creating blank local revisions to fill
            // in the local history:
            long sequence = 0;
            long localParentSequence = 0;
            for (int i = history.size() - 1; i >= 0; --i) {
                revId = history.get(i);
                RevisionInternal localRev = (localRevs != null) ? localRevs.get(revId) : null;
                if (localRev != null) {
                    // This revision is known locally. Remember its sequence as the parent of the next one:
                    sequence = localRev.getSequence();
                    assert (sequence > 0);
                    localParentSequence = sequence;
                } else {
                    // This revision isn't known, so add it:

                    RevisionInternal newRev;
                    byte[] data = null;
                    boolean current = false;
                    if (i == 0) {
                        // Hey, this is the leaf revision we're inserting:
                        newRev = inRev;
                        if (!inRev.isDeleted()) {
                            data = RevisionUtils.encodeDocumentJSON(inRev);
                            if (data == null) {
                                throw new CouchbaseLiteException(Status.BAD_REQUEST);
                            }
                        }
                        current = true;
                    } else {
                        // It's an intermediate parent, so insert a stub:
                        newRev = new RevisionInternal(docId, revId, false);
                    }

                    // Insert it:
                    sequence = insertRevision(newRev, docNumericID, sequence, current, (newRev.getAttachments() != null && newRev.getAttachments().size() > 0), data);

                    if (sequence <= 0) {
                        throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                    }

                    if (i == 0) {
                        // Write any changed attachments for the new revision. As the parent sequence use
                        // the latest local revision (this is to copy attachments from):
                        String prevRevID = (history.size() >= 2) ? history.get(1) : null;
                        Map<String, AttachmentInternal> attachments = delegate.getAttachmentsFromRevision(inRev, prevRevID);
                        if (attachments != null) {
                            delegate.processAttachmentsForRevision(attachments, inRev, localParentSequence);
                            delegate.stubOutAttachmentsInRevision(attachments, inRev);
                        }
                    }
                }
            }

            // Mark the latest local rev as no longer current:
            if (localParentSequence > 0 && localParentSequence != sequence) {
                ContentValues args = new ContentValues();
                args.put("current", 0);
                String[] whereArgs = {Long.toString(localParentSequence)};
                int numRowsChanged = 0;
                try {
                    numRowsChanged = storageEngine.update("revs", args, "sequence=? AND current!=0", whereArgs);
                    if (numRowsChanged == 0) {
                        inConflict = true;  // local parent wasn't a leaf, ergo we just created a branch
                    }
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
                }
            }

            winningRev = delegate.winner(docNumericID, oldWinningRevID, oldWinnerWasDeletion, inRev);

            success = true;

            // Notify and return:
            if (delegate != null)
                delegate.databaseStorageChanged(new DocumentChange(inRev, winningRev, inConflict, source));

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(success);
        }
    }

    /**
     * Purges specific revisions, which deletes them completely from the local storageEngine _without_ adding a "tombstone" revision. It's as though they were never there.
     * This operation is described here: http://wiki.apache.org/couchdb/Purge_Documents
     *
     * @param docsToRevs A dictionary mapping document IDs to arrays of revision IDs.
     * @resultOn success will point to an NSDictionary with the same form as docsToRev, containing the doc/revision IDs that were actually removed.
     */
    @Override
    @InterfaceAudience.Private
    public Map<String, Object> purgeRevisions(final Map<String, List<String>> docsToRevs) {

        final Map<String, Object> result = new HashMap<String, Object>();
        runInTransaction(new TransactionalTask() {
            @Override
            public boolean run() {
                for (String docID : docsToRevs.keySet()) {
                    long docNumericID = getDocNumericID(docID);
                    if (docNumericID == -1) {
                        continue; // no such document, skip it
                    }
                    List<String> revsPurged = new ArrayList<String>();
                    List<String> revIDs = (List<String>) docsToRevs.get(docID);
                    if (revIDs == null) {
                        return false;
                    } else if (revIDs.size() == 0) {
                        revsPurged = new ArrayList<String>();
                    } else if (revIDs.contains("*")) {
                        // Delete all revisions if magic "*" revision ID is given:
                        try {
                            String[] args = {Long.toString(docNumericID)};
                            storageEngine.execSQL("DELETE FROM revs WHERE doc_id=?", args);
                        } catch (SQLException e) {
                            Log.e(TAG, "Error deleting revisions", e);
                            return false;
                        }
                        revsPurged = new ArrayList<String>();
                        revsPurged.add("*");
                    } else {
                        // Iterate over all the revisions of the doc, in reverse sequence order.
                        // Keep track of all the sequences to delete, i.e. the given revs and ancestors,
                        // but not any non-given leaf revs or their ancestors.
                        Cursor cursor = null;

                        try {
                            String[] args = {Long.toString(docNumericID)};
                            String queryString = "SELECT revid, sequence, parent FROM revs WHERE doc_id=? ORDER BY sequence DESC";
                            cursor = storageEngine.rawQuery(queryString, args);
                            if (!cursor.moveToNext()) {
                                Log.w(TAG, "No results for query: %s", queryString);
                                return false;
                            }

                            Set<Long> seqsToPurge = new HashSet<Long>();
                            Set<Long> seqsToKeep = new HashSet<Long>();
                            Set<String> revsToPurge = new HashSet<String>();
                            while (!cursor.isAfterLast()) {

                                String revID = cursor.getString(0);
                                long sequence = cursor.getLong(1);
                                long parent = cursor.getLong(2);
                                if (seqsToPurge.contains(sequence) || revIDs.contains(revID) && !seqsToKeep.contains(sequence)) {
                                    // Purge it and maybe its parent:
                                    seqsToPurge.add(sequence);
                                    revsToPurge.add(revID);
                                    if (parent > 0) {
                                        seqsToPurge.add(parent);
                                    }
                                } else {
                                    // Keep it and its parent:
                                    seqsToPurge.remove(sequence);
                                    revsToPurge.remove(revID);
                                    seqsToKeep.add(parent);
                                }

                                cursor.moveToNext();
                            }

                            seqsToPurge.removeAll(seqsToKeep);
                            Log.i(TAG, "Purging doc '%s' revs (%s); asked for (%s)", docID, revsToPurge, revIDs);
                            if (seqsToPurge.size() > 0) {
                                // Now delete the sequences to be purged.
                                String seqsToPurgeList = TextUtils.join(",", seqsToPurge);
                                String sql = String.format("DELETE FROM revs WHERE sequence in (%s)", seqsToPurgeList);
                                try {
                                    storageEngine.execSQL(sql);
                                } catch (SQLException e) {
                                    Log.e(TAG, "Error deleting revisions via: " + sql, e);
                                    return false;
                                }
                            }
                            revsPurged.addAll(revsToPurge);

                        } catch (SQLException e) {
                            Log.e(TAG, "Error getting revisions", e);
                            return false;
                        } finally {
                            if (cursor != null) {
                                cursor.close();
                            }
                        }
                    }
                    result.put(docID, revsPurged);
                }
                return true;
            }
        });

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // VIEWS:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Instantiates storage for a view.
     *
     * @param name   The name of the view
     * @param create If YES, the view should be created; otherwise it must already exist
     * @return Storage for the view, or nil if create=NO and it doesn't exist.
     */
    public ViewStore getViewStorage(String name, boolean create){
        return new SQLiteViewStore(this,name, create);
    }

    @Override
    public List<String> getAllViewNames(){
        Cursor cursor = null;
        List<String> result = null;

        try {
            cursor = storageEngine.rawQuery("SELECT name FROM views", null);
            cursor.moveToNext();
            result = new ArrayList<String>();
            while (!cursor.isAfterLast()) {
                //result.add(delegate.getView(cursor.getString(0)));
                result.add(cursor.getString(0));
                cursor.moveToNext();
            }
        } catch (Exception e) {
            Log.e(TAG, "Error getting all views", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////
    // LOCAL DOCS:
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public RevisionInternal getLocalDocument(String docID, String revID) {
        // docID already should contain "_local/" prefix
        RevisionInternal result = null;
        Cursor cursor = null;
        try {
            String[] args = {docID};
            cursor = storageEngine.rawQuery("SELECT revid, json FROM localdocs WHERE docid=?", args);
            if (cursor.moveToNext()) {
                String gotRevID = cursor.getString(0);
                if (revID != null && (!revID.equals(gotRevID))) {
                    return null;
                }
                byte[] json = cursor.getBlob(1);
                Map<String, Object> properties = null;
                try {
                    properties = Manager.getObjectMapper().readValue(json, Map.class);
                    properties.put("_id", docID);
                    properties.put("_rev", gotRevID);
                    result = new RevisionInternal(docID, gotRevID, false);
                    result.setProperties(properties);
                } catch (Exception e) {
                    Log.w(TAG, "Error parsing local doc JSON", e);
                    return null;
                }

            }
            return result;
        } catch (SQLException e) {
            Log.e(TAG, "Error getting local document", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID) throws CouchbaseLiteException {
        String docID = revision.getDocId();
        if (!docID.startsWith("_local/")) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        if (!revision.isDeleted()) {
            // PUT:
            byte[] json = RevisionUtils.encodeDocumentJSON(revision);
            String newRevID;
            if (prevRevID != null) {
                int generation = RevisionInternal.generationFromRevID(prevRevID);
                if (generation == 0) {
                    throw new CouchbaseLiteException(Status.BAD_REQUEST);
                }
                newRevID = Integer.toString(++generation) + "-local";
                ContentValues values = new ContentValues();
                values.put("revid", newRevID);
                values.put("json", json);
                String[] whereArgs = {docID, prevRevID};
                try {
                    int rowsUpdated = storageEngine.update("localdocs", values, "docid=? AND revid=?", whereArgs);
                    if (rowsUpdated == 0) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            } else {
                newRevID = "1-local";
                ContentValues values = new ContentValues();
                values.put("docid", docID);
                values.put("revid", newRevID);
                values.put("json", json);
                try {
                    storageEngine.insertWithOnConflict("localdocs", null, values, SQLiteStorageEngine.CONFLICT_IGNORE);
                } catch (SQLException e) {
                    throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
                }
            }
            return revision.copyWithDocID(docID, newRevID);
        } else {
            // DELETE:
            deleteLocalDocument(docID, prevRevID);
            return revision;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // temporary from here
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public SQLiteStorageEngine getStorageEngine() {
        return storageEngine;
    }

    /**
     * Begins a storageEngine transaction. Transactions can nest.
     * Every beginTransaction() must be balanced by a later endTransaction()
     * <p/>
     * Notes: 1. SQLiteDatabase.beginTransaction() supported nested transaction. But, in case
     * nested transaction rollbacked, it also rollbacks outer transaction too.
     * This is not ideal behavior for CBL
     * 2. SAVEPOINT...RELEASE supports nested transaction. But Android API 14 and 15,
     * it throws Exception. I assume it is Android bug. As CBL need to support from API 10
     * . So it does not work for CBL.
     * 3. Using Transaction for outer/1st level of transaction and inner/2nd level of transaction
     * works with CBL requirement.
     * 4. CBL Android and Java uses Thread, So it is better to use SQLiteDatabase.beginTransaction()
     * for outer/1st level transaction. if we use BEGIN...COMMIT and SAVEPOINT...RELEASE,
     * we need to implement wrapper of BEGIN...COMMIT and SAVEPOINT...RELEASE to be
     * Thread-safe.
     */
    @Override
    public boolean beginTransaction() {
        int tLevel = transactionLevel.get();
        //Log.v(Log.TAG_DATABASE, "[Database.beginTransaction()] Database=" + this + ", SQLiteStorageEngine=" + storageEngine + ", transactionLevel=" + transactionLevel + ", currentThread=" + Thread.currentThread());
        try {
            // Outer (level 0)  transaction. Use SQLiteDatabase.beginTransaction()
            if (tLevel == 0) {
                storageEngine.beginTransaction();
            }
            // Inner (level 1 or higher) transaction. Use SQLite's SAVEPOINT
            else {
                storageEngine.execSQL("SAVEPOINT cbl_" + Integer.toString(tLevel));
            }
            Log.v(Log.TAG_DATABASE, "%s Begin transaction (level %d)", Thread.currentThread().getName(), tLevel);
            transactionLevel.set(++tLevel);
        } catch (SQLException e) {
            Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling beginTransaction()", e);
            return false;
        }
        return true;
    }

    /**
     * Commits or aborts (rolls back) a transaction.
     *
     * @param commit If true, commits; if false, aborts and rolls back, undoing all changes made since the matching -beginTransaction call, *including* any committed nested transactions.
     * @exclude
     */
    @Override
    public boolean endTransaction(boolean commit) {
        int tLevel = transactionLevel.get();

        assert (tLevel > 0);

        transactionLevel.set(--tLevel);

        // Outer (level 0) transaction. Use SQLiteDatabase.setTransactionSuccessful() and SQLiteDatabase.endTransaction()
        if (tLevel == 0) {
            if (commit) {
                Log.v(Log.TAG_DATABASE, "%s Committing transaction (level %d)", Thread.currentThread().getName(), tLevel);
                storageEngine.setTransactionSuccessful();
                storageEngine.endTransaction();
            } else {
                Log.v(Log.TAG_DATABASE, "%s CANCEL transaction (level %d)", Thread.currentThread().getName(), tLevel);
                try {
                    storageEngine.endTransaction();
                } catch (SQLException e) {
                    Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                    return false;
                }
            }
        }
        // Inner (level 1 or higher) transaction: Use SQLite's ROLLBACK and RELEASE
        else {
            if (commit) {
                Log.v(Log.TAG_DATABASE, "%s Committing transaction (level %d)", Thread.currentThread().getName(), tLevel);
            } else {
                Log.v(Log.TAG_DATABASE, "%s CANCEL transaction (level %d)", Thread.currentThread().getName(), tLevel);
                try {
                    storageEngine.execSQL(";ROLLBACK TO cbl_" + Integer.toString(tLevel));
                } catch (SQLException e) {
                    Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                    return false;
                }
            }
            try {
                storageEngine.execSQL("RELEASE cbl_" + Integer.toString(tLevel));
            } catch (SQLException e) {
                Log.e(Log.TAG_DATABASE, Thread.currentThread().getName() + " Error calling endTransaction()", e);
                return false;
            }
        }

        if (delegate != null)
            delegate.storageExitedTransaction(commit);

        return true;
    }

    /**
     * https://github.com/couchbase/couchbase-lite-ios/issues/615
     */
    @Override
    public void optimizeSQLIndexes() {
        Log.v(Log.TAG_DATABASE, "calls optimizeSQLIndexes()");
        final long currentSequence = getLastSequence();
        if (currentSequence > 0) {
            final long lastOptimized = getLastOptimized();
            if (lastOptimized <= currentSequence / 10) {
                runInTransaction(new TransactionalTask() {
                    @Override
                    public boolean run() {
                        Log.i(Log.TAG_DATABASE, "%s: Optimizing SQL indexes (curSeq=%d, last run at %d)", this, currentSequence, lastOptimized);
                        storageEngine.execSQL("ANALYZE");
                        storageEngine.execSQL("ANALYZE sqlite_master");
                        setInfo("last_optimized", String.valueOf(currentSequence));
                        return true;
                    }
                });
            }
        }
    }

    @Override
    public String publicUUID() {
        String result = null;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error querying privateUUID", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public String privateUUID() {
        String result = null;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery("SELECT value FROM info WHERE key='privateUUID'", null);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error querying privateUUID", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
     */
    @Override
    public Map<String, Object> getAttachmentsDictForSequenceWithContent(long sequence, EnumSet<Database.TDContentOptions> contentOptions) {
        assert (sequence > 0);

        Cursor cursor = null;

        String args[] = {Long.toString(sequence)};
        try {
            cursor = storageEngine.rawQuery("SELECT filename, key, type, encoding, length, encoded_length, revpos FROM attachments WHERE sequence=?", args);

            if (!cursor.moveToNext()) {
                return null;
            }

            Map<String, Object> result = new HashMap<String, Object>();

            while (!cursor.isAfterLast()) {

                boolean dataSuppressed = false;
                int length = cursor.getInt(4);

                AttachmentInternal.AttachmentEncoding encoding = AttachmentInternal.AttachmentEncoding.values()[cursor.getInt(3)];
                int encodedLength = cursor.getInt(5);

                byte[] keyData = cursor.getBlob(1);
                BlobKey key = new BlobKey(keyData);
                String digestString = "sha1-" + Base64.encodeBytes(keyData);
                String dataBase64 = null;
                if (contentOptions.contains(Database.TDContentOptions.TDIncludeAttachments)) {
                    if (contentOptions.contains(Database.TDContentOptions.TDBigAttachmentsFollow) &&
                            length >= Database.kBigAttachmentLength) {
                        dataSuppressed = true;
                    } else {
                        byte[] data = delegate.getAttachments().blobForKey(key);
                        if (data != null) {
                            dataBase64 = Base64.encodeBytes(data);  // <-- very expensive
                        } else {
                            Log.w(TAG, "Error loading attachment.  Sequence: %s", sequence);
                        }
                    }
                }

                String encodingStr = null;
                if (encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) {
                    // NOTE: iOS decode if attachment is included int the dict.
                    encodingStr = "gzip";
                }

                Map<String, Object> attachment = new HashMap<String, Object>();
                if (!(dataBase64 != null || dataSuppressed)) {
                    attachment.put("stub", true);
                }
                if (dataBase64 != null) {
                    attachment.put("data", dataBase64);
                }
                if (dataSuppressed == true) {
                    attachment.put("follows", true);
                }
                attachment.put("digest", digestString);
                String contentType = cursor.getString(2);
                attachment.put("content_type", contentType);
                if (encodingStr != null) {
                    attachment.put("encoding", encodingStr);
                }
                attachment.put("length", length);
                if (encodingStr != null && encodedLength >= 0) {
                    attachment.put("encoded_length", encodedLength);
                }
                attachment.put("revpos", cursor.getInt(6));

                String filename = cursor.getString(0);
                result.put(filename, attachment);

                cursor.moveToNext();
            }

            return result;

        } catch (SQLException e) {
            Log.e(TAG, "Error getting attachments for sequence", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public RevisionList getAllRevisions(String docID, boolean onlyCurrent) {
        long docNumericId = getDocNumericID(docID);
        if (docNumericId < 0) {
            return null;
        } else if (docNumericId == 0) {
            return new RevisionList();
        } else {
            return getAllRevisions(docID, docNumericId, onlyCurrent);
        }
    }

    @Override
    public long getDocNumericID(String docId) {
        Cursor cursor = null;
        String[] args = {docId};

        long result = -1;
        try {
            cursor = storageEngine.rawQuery("SELECT doc_id FROM docs WHERE docid=?", args);

            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            } else {
                result = 0;
            }
        } catch (Exception e) {
            Log.e(TAG, "Error getting doc numeric id", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return result;
    }

    /**
     * Deletes obsolete attachments from the sqliteDb and blob store.
     */
    @Override
    public Status garbageCollectAttachments() {
        // First delete attachment rows for already-cleared revisions:
        // OPT: Could start after last sequence# we GC'd up to

        try {
            storageEngine.execSQL("DELETE FROM attachments WHERE sequence IN " +
                    "(SELECT sequence from revs WHERE json IS null)");
        } catch (SQLException e) {
            Log.e(TAG, "Error deleting attachments", e);
        }

        // Now collect all remaining attachment IDs and tell the store to delete all but these:
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery("SELECT DISTINCT key FROM attachments", null);

            cursor.moveToNext();
            List<BlobKey> allKeys = new ArrayList<BlobKey>();
            while (!cursor.isAfterLast()) {
                BlobKey key = new BlobKey(cursor.getBlob(0));
                allKeys.add(key);
                cursor.moveToNext();
            }

            int numDeleted = delegate.getAttachments().deleteBlobsExceptWithKeys(allKeys);
            if (numDeleted < 0) {
                return new Status(Status.INTERNAL_SERVER_ERROR);
            }

            Log.v(TAG, "Deleted %d attachments", numDeleted);

            return new Status(Status.OK);
        } catch (SQLException e) {
            Log.e(TAG, "Error finding attachment keys in use", e);
            return new Status(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
     * <p/>
     * in CBLDatabase+Internal.m
     * - (NSString*) winningRevIDOfDocNumericID: (SInt64)docNumericID
     * isDeleted: (BOOL*)outIsDeleted
     * isConflict: (BOOL*)outIsConflict // optional
     * status: (CBLStatus*)outStatus
     */
    @Override
    public String winningRevIDOfDoc(long docNumericId, AtomicBoolean outIsDeleted, AtomicBoolean outIsConflict) throws CouchbaseLiteException {

        Cursor cursor = null;
        String sql = "SELECT revid, deleted FROM revs" +
                " WHERE doc_id=? and current=1" +
                " ORDER BY deleted asc, revid desc LIMIT 2";

        String[] args = {Long.toString(docNumericId)};
        String revId = null;

        try {
            cursor = storageEngine.rawQuery(sql, args);

            if (cursor.moveToNext()) {
                revId = cursor.getString(0);
                outIsDeleted.set(cursor.getInt(1) > 0);
                // The document is in conflict if there are two+ result rows that are not deletions.
                if (outIsConflict != null) {
                    outIsConflict.set(!outIsDeleted.get() && cursor.moveToNext() && !(cursor.getInt(1) > 0));
                }
            } else {
                outIsDeleted.set(false);
                if (outIsConflict != null) {
                    outIsConflict.set(false);
                }
            }

        } catch (SQLException e) {
            Log.e(TAG, "Error", e);
            throw new CouchbaseLiteException("Error", e, new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return revId;
    }

    @Override
    public boolean existsDocumentWithIDAndRev(String docId, String revId) {
        return getDocument(docId, revId, EnumSet.of(Database.TDContentOptions.TDNoBody)) != null;
    }



    @Override
    public void copyAttachmentNamedFromSequenceToSequence(String name, long fromSeq, long toSeq) throws CouchbaseLiteException {
        assert (name != null);
        assert (toSeq > 0);
        if (fromSeq < 0) {
            throw new CouchbaseLiteException(Status.NOT_FOUND);
        }

        Cursor cursor = null;

        String[] args = {Long.toString(toSeq), name, Long.toString(fromSeq), name};
        try {
            storageEngine.execSQL("INSERT INTO attachments (sequence, filename, key, type, length, revpos) " +
                    "SELECT ?, ?, key, type, length, revpos FROM attachments " +
                    "WHERE sequence=? AND filename=?", args);
            cursor = storageEngine.rawQuery("SELECT changes()", null);
            cursor.moveToNext();
            int rowsUpdated = cursor.getInt(0);
            if (rowsUpdated == 0) {
                // Oops. This means a glitch in our attachment-management or pull code,
                // or else a bug in the upstream server.
                Log.w(TAG, "Can't find inherited attachment %s from seq# %s to copy to %s", name, fromSeq, toSeq);
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            } else {
                return;
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error copying attachment", e);
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public String lastSequenceWithCheckpointId(String checkpointId) {
        Cursor cursor = null;
        String result = null;
        try {
            // This table schema is out of date but I'm keeping it the way it is for compatibility.
            // The 'remote' column now stores the opaque checkpoint IDs, and 'push' is ignored.
            String[] args = {checkpointId};
            cursor = storageEngine.rawQuery("SELECT last_sequence FROM replicators WHERE remote=?", args);
            if (cursor.moveToNext()) {
                result = cursor.getString(0);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting last sequence", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public void deleteLocalDocument(String docID, String revID) throws CouchbaseLiteException {
        if (docID == null) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }
        if (revID == null) {
            // Didn't specify a revision to delete: 404 or a 409, depending
            if (getLocalDocument(docID, null) != null) {
                throw new CouchbaseLiteException(Status.CONFLICT);
            } else {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }
        }
        String[] whereArgs = {docID, revID};
        try {
            int rowsDeleted = storageEngine.delete("localdocs", "docid=? AND revid=?", whereArgs);
            if (rowsDeleted == 0) {
                if (getLocalDocument(docID, null) != null) {
                    throw new CouchbaseLiteException(Status.CONFLICT);
                } else {
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                }
            }
        } catch (SQLException e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public boolean replaceUUIDs() {
        String query = "UPDATE INFO SET value='" + Misc.CreateUUID() + "' where key = 'privateUUID';";
        try {
            storageEngine.execSQL(query);
        } catch (SQLException e) {
            Log.e(TAG, "Error updating UUIDs", e);
            return false;
        }
        query = "UPDATE INFO SET value='" + Misc.CreateUUID() + "' where key = 'publicUUID';";
        try {
            storageEngine.execSQL(query);
        } catch (SQLException e) {
            Log.e(TAG, "Error updating UUIDs", e);
            return false;
        }
        return true;
    }

    /*
    @Override
    public Status deleteViewNamed(String name) {
        Status result = new Status(Status.INTERNAL_SERVER_ERROR);
        try {
            if (delegate.getViews() != null) {
                if (name != null) {
                    delegate.getViews().remove(name);
                }
            }
            String[] whereArgs = {name};
            int rowsAffected = storageEngine.delete("views", "name=?", whereArgs);
            if (rowsAffected > 0) {
                result.setCode(Status.OK);
            } else {
                result.setCode(Status.NOT_FOUND);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error deleting view", e);
        }
        return result;
    }
    */

    @Override
    public Map<String, Object> documentPropertiesFromJSON(byte[] json, String docId, String revId, boolean deleted, long sequence, EnumSet<Database.TDContentOptions> contentOptions) {

        RevisionInternal rev = new RevisionInternal(docId, revId, deleted);
        rev.setSequence(sequence);
        Map<String, Object> extra = extraPropertiesForRevision(rev, contentOptions);
        if (json == null) {
            return extra;
        }

        Map<String, Object> docProperties = null;
        try {
            docProperties = Manager.getObjectMapper().readValue(json, Map.class);
            docProperties.putAll(extra);
            return docProperties;
        } catch (Exception e) {
            Log.e(TAG, "Error serializing properties to JSON", e);
        }

        return docProperties;
    }

    /**
     * Returns the content and MIME type of an attachment
     */
    @Override
    public Attachment getAttachmentForSequence(long sequence, String filename) throws CouchbaseLiteException {
        assert (sequence > 0);
        assert (filename != null);

        Cursor cursor = null;

        String[] args = {Long.toString(sequence), filename};
        try {
            cursor = storageEngine.rawQuery("SELECT key, type FROM attachments WHERE sequence=? AND filename=?", args);

            if (!cursor.moveToNext()) {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }

            byte[] keyData = cursor.getBlob(0);
            //TODO add checks on key here? (ios version)
            BlobKey key = new BlobKey(keyData);
            InputStream contentStream = delegate.getAttachments().blobStreamForKey(key);
            if (contentStream == null) {
                Log.e(TAG, "Failed to load attachment");
                throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
            } else {
                Attachment result = new Attachment(contentStream, cursor.getString(1));
                result.setGZipped(delegate.getAttachments().isGZipped(key));
                return result;
            }

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Returns the location of an attachment's file in the blob store.
     */
    @Override
    public String getAttachmentPathForSequence(long sequence, String filename) throws CouchbaseLiteException {
        assert (sequence > 0);
        assert (filename != null);
        Cursor cursor = null;
        String filePath = null;

        String args[] = {Long.toString(sequence), filename};
        try {
            cursor = storageEngine.rawQuery("SELECT key, type, encoding FROM attachments WHERE sequence=? AND filename=?", args);

            if (!cursor.moveToNext()) {
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            }

            byte[] keyData = cursor.getBlob(0);
            BlobKey key = new BlobKey(keyData);
            filePath = delegate.getAttachments().pathForKey(key);
            return filePath;

        } catch (SQLException e) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public boolean setLastSequence(String lastSequence, String checkpointId, boolean push) {
        Log.v(TAG, "%s: setLastSequence() called with lastSequence: %s checkpointId: %s", this, lastSequence, checkpointId);
        ContentValues values = new ContentValues();
        values.put("remote", checkpointId);
        values.put("push", push);
        values.put("last_sequence", lastSequence);
        long newId = storageEngine.insertWithOnConflict("replicators", null, values, SQLiteStorageEngine.CONFLICT_REPLACE);
        return (newId == -1);
    }

    @Override
    public void insertAttachmentForSequenceWithNameAndType(long sequence, String name, String contentType, int revpos, BlobKey key, long length, AttachmentInternal.AttachmentEncoding encoding, long encodedLength) throws CouchbaseLiteException {
        try {
            ContentValues args = new ContentValues();
            args.put("sequence", sequence);
            args.put("filename", name);
            args.put("type", contentType);
            args.put("revpos", revpos);
            if (key != null) {
                args.put("key", key.getBytes());
            }
            if (length >= 0) {
                args.put("length", length);
            }
            if (encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) {
                args.put("encoding", encoding.ordinal());
                if (encodedLength >= 0) {
                    args.put("encoded_length", encodedLength);
                }
            }
            long result = storageEngine.insert("attachments", null, args);
            if (result == -1) {
                String msg = "Insert attachment failed (returned -1)";
                Log.e(TAG, msg);
                throw new CouchbaseLiteException(msg, Status.INTERNAL_SERVER_ERROR);
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error inserting attachment", e);
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (PROTECTED & PRIVATE) METHODS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Prune revisions to the given max depth.  Eg, remove revisions older than that max depth,
     * which will reduce storage requirements.
     * <p/>
     * TODO: This implementation is a bit simplistic. It won't do quite the right thing in
     * histories with branches, if one branch stops much earlier than another. The shorter branch
     * will be deleted entirely except for its leaf revision. A more accurate pruning
     * would require an expensive full tree traversal. Hopefully this way is good enough.
     */
    protected int pruneRevsToMaxDepth(int maxDepth) throws CouchbaseLiteException {

        int outPruned = 0;
        boolean shouldCommit = false;
        Map<Long, Integer> toPrune = new HashMap<Long, Integer>();

        if (maxDepth == 0) {
            maxDepth = getMaxRevTreeDepth();
        }

        // First find which docs need pruning, and by how much:

        Cursor cursor = null;
        String[] args = {};

        long docNumericID = -1;
        int minGen = 0;
        int maxGen = 0;

        try {

            cursor = storageEngine.rawQuery("SELECT doc_id, MIN(revid), MAX(revid) FROM revs GROUP BY doc_id", args);

            while (cursor.moveToNext()) {
                docNumericID = cursor.getLong(0);
                String minGenRevId = cursor.getString(1);
                String maxGenRevId = cursor.getString(2);
                minGen = Revision.generationFromRevID(minGenRevId);
                maxGen = Revision.generationFromRevID(maxGenRevId);
                if ((maxGen - minGen + 1) > maxDepth) {
                    toPrune.put(docNumericID, (maxGen - minGen));
                }
            }

            beginTransaction();

            if (toPrune.size() == 0) {
                return 0;
            }

            for (Long docNumericIDLong : toPrune.keySet()) {
                String minIDToKeep = String.format("%d-", toPrune.get(docNumericIDLong).intValue() + 1);
                String[] deleteArgs = {Long.toString(docNumericID), minIDToKeep};
                int rowsDeleted = storageEngine.delete("revs", "doc_id=? AND revid < ? AND current=0", deleteArgs);
                outPruned += rowsDeleted;
            }

            shouldCommit = true;

        } catch (Exception e) {
            throw new CouchbaseLiteException(e, Status.INTERNAL_SERVER_ERROR);
        } finally {
            endTransaction(shouldCommit);
            if (cursor != null) {
                cursor.close();
            }
        }

        return outPruned;
    }

    private boolean initialize(String statements) {
        try {
            for (String statement : statements.split(";")) {
                storageEngine.execSQL(statement);
            }
        } catch (SQLException e) {
            close();
            return false;
        }
        return true;
    }

    private long getLastOptimized() {
        String info = getInfo("last_optimized");
        if (info != null) {
            return Long.parseLong(info);
        }
        return 0;
    }

    /**
     * Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
     * Rev must already have its revID and sequence properties set.
     */
    private void expandStoredJSONIntoRevisionWithAttachments(byte[] json, RevisionInternal rev, EnumSet<Database.TDContentOptions> contentOptions) {
        Map<String, Object> extra = extraPropertiesForRevision(rev, contentOptions);
        if (json != null && json.length > 0) {
            rev.setJson(appendDictToJSON(json, extra));
        } else {
            rev.setProperties(extra);
            if (json == null)
                rev.setMissing(true);
        }
    }

    /**
     * Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
     */
    private byte[] appendDictToJSON(byte[] json, Map<String, Object> dict) {
        if (dict.size() == 0) {
            return json;
        }

        byte[] extraJSON = null;
        try {
            extraJSON = Manager.getObjectMapper().writeValueAsBytes(dict);
        } catch (Exception e) {
            Log.e(TAG, "Error convert extra JSON to bytes", e);
            return null;
        }

        int jsonLength = json.length;
        int extraLength = extraJSON.length;
        if (jsonLength == 2) { // Original JSON was empty
            return extraJSON;
        }
        byte[] newJson = new byte[jsonLength + extraLength - 1];
        System.arraycopy(json, 0, newJson, 0, jsonLength - 1);  // Copy json w/o trailing '}'
        newJson[jsonLength - 1] = ',';  // Add a ','
        System.arraycopy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
        return newJson;
    }

    /**
     * Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
     * Rev must already have its revID and sequence properties set.
     */
    private Map<String, Object> extraPropertiesForRevision(RevisionInternal rev, EnumSet<Database.TDContentOptions> contentOptions) {

        String docId = rev.getDocId();
        String revId = rev.getRevId();
        long sequenceNumber = rev.getSequence();
        assert (revId != null);
        assert (sequenceNumber > 0);

        Map<String, Object> attachmentsDict = null;
        // Get attachment metadata, and optionally the contents:
        if (!contentOptions.contains(Database.TDContentOptions.TDNoAttachments)) {
            attachmentsDict = getAttachmentsDictForSequenceWithContent(sequenceNumber, contentOptions);
        }

        // Get more optional stuff to put in the properties:
        //OPT: This probably ends up making redundant SQL queries if multiple options are enabled.
        Long localSeq = null;
        if (contentOptions.contains(Database.TDContentOptions.TDIncludeLocalSeq)) {
            localSeq = sequenceNumber;
        }

        Map<String, Object> revHistory = null;
        if (contentOptions.contains(Database.TDContentOptions.TDIncludeRevs)) {
            revHistory = RevisionUtils.makeRevisionHistoryDict(getRevisionHistory(rev));
        }

        List<Object> revsInfo = null;
        if (contentOptions.contains(Database.TDContentOptions.TDIncludeRevsInfo)) {
            revsInfo = new ArrayList<Object>();
            List<RevisionInternal> revHistoryFull = getRevisionHistory(rev);
            for (RevisionInternal historicalRev : revHistoryFull) {
                Map<String, Object> revHistoryItem = new HashMap<String, Object>();
                String status = "available";
                if (historicalRev.isDeleted()) {
                    status = "deleted";
                }
                if (historicalRev.isMissing()) {
                    status = "missing";
                }
                revHistoryItem.put("rev", historicalRev.getRevId());
                revHistoryItem.put("status", status);
                revsInfo.add(revHistoryItem);
            }
        }

        List<String> conflicts = null;
        if (contentOptions.contains(Database.TDContentOptions.TDIncludeConflicts)) {
            RevisionList revs = getAllRevisions(docId, true);
            if (revs.size() > 1) {
                conflicts = new ArrayList<String>();
                for (RevisionInternal aRev : revs) {
                    if (aRev.equals(rev) || aRev.isDeleted()) {
                        // don't add in this case
                    } else {
                        conflicts.add(aRev.getRevId());
                    }
                }
            }
        }

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("_id", docId);
        result.put("_rev", revId);
        if (rev.isDeleted()) {
            result.put("_deleted", true);
        }
        if (attachmentsDict != null) {
            result.put("_attachments", attachmentsDict);
        }
        if (localSeq != null) {
            result.put("_local_seq", localSeq);
        }
        if (revHistory != null) {
            result.put("_revisions", revHistory);
        }
        if (revsInfo != null) {
            result.put("_revs_info", revsInfo);
        }
        if (conflicts != null) {
            result.put("_conflicts", conflicts);
        }

        return result;
    }

    private boolean sequenceHasAttachments(long sequence) {
        Cursor cursor = null;

        String args[] = {Long.toString(sequence)};
        try {
            cursor = storageEngine.rawQuery("SELECT 1 FROM attachments WHERE sequence=? LIMIT 1", args);
            if (cursor.moveToNext()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            Log.e(TAG, "Error getting attachments for sequence", e);
            return false;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    private long getOrInsertDocNumericID(String docId) {
        long docNumericId = getDocNumericID(docId);
        if (docNumericId == 0) {
            docNumericId = insertDocumentID(docId);
        }
        return docNumericId;
    }

    private long insertDocumentID(String docId) {
        long rowId = -1;
        try {
            ContentValues args = new ContentValues();
            args.put("docid", docId);
            rowId = storageEngine.insert("docs", null, args);
        } catch (Exception e) {
            Log.e(TAG, "Error inserting document id", e);
        }
        return rowId;
    }

    private long insertRevision(RevisionInternal rev, long docNumericID, long parentSequence, boolean current, boolean hasAttachments, byte[] data) {
        long rowId = 0;
        try {
            ContentValues args = new ContentValues();
            args.put("doc_id", docNumericID);
            args.put("revid", rev.getRevId());
            if (parentSequence != 0) {
                args.put("parent", parentSequence);
            }
            args.put("current", current);
            args.put("deleted", rev.isDeleted());
            args.put("no_attachments", !hasAttachments);
            args.put("json", data);
            rowId = storageEngine.insert("revs", null, args);
            rev.setSequence(rowId);
        } catch (Exception e) {
            Log.e(TAG, "Error inserting revision", e);
        }
        return rowId;
    }

    private long getSequenceOfDocument(long docNumericId, String revId, boolean onlyCurrent) {
        long result = -1;
        Cursor cursor = null;
        try {
            String extraSql = (onlyCurrent ? "AND current=1" : "");
            String sql = String.format("SELECT sequence FROM revs WHERE doc_id=? AND revid=? %s LIMIT 1", extraSql);
            String[] args = {"" + docNumericId, revId};
            cursor = storageEngine.rawQuery(sql, args);

            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            } else {
                result = 0;
            }
        } catch (Exception e) {
            Log.e(TAG, "Error getting getSequenceOfDocument", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    /**
     * Hack because cursor interface does not support cursor.getColumnIndex("deleted") yet.
     */
    private int getDeletedColumnIndex(QueryOptions options) {
        if (options.isIncludeDocs()) {
            return 5;
        } else {
            return 4;
        }
    }

    private long longForQuery(String sqlQuery, String[] args) throws SQLException {
        Cursor cursor = null;
        long result = 0;
        try {
            cursor = storageEngine.rawQuery(sqlQuery, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }
}