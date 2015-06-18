package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.ContentValues;
import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.support.JsonDocument;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLiteViewStorage implements ViewStorage {

    private abstract class AbstractMapEmitBlock implements Emitter {
        protected long sequence = 0;

        void setSequence(long sequence) {
            this.sequence = sequence;
        }
    }

    private static final int REDUCE_BATCH_SIZE = 100;

    private String name;
    private SQLiteStorage dbStorage;
    private int viewId;
    private ViewStorageDelegate delegate;
    private Database db;
    private View.TDViewCollation collation;

    public SQLiteViewStorage(String name, SQLiteStorage dbStorage, ViewStorageDelegate delegate, Database db) {
        this.name = name;
        this.dbStorage = dbStorage;
        this.viewId = -1; // means 'unknown'
        this.delegate = delegate;
        this.db = db;
        this.collation = View.TDViewCollation.TDViewCollationUnicode;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of ViewStorage
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ViewStorageDelegate getDelegate() {
        return delegate;
    }

    @Override
    public void setDelegate(ViewStorageDelegate delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setCollation(View.TDViewCollation collation) {
        this.collation = collation;
    }

    @Override
    public void close() {
        dbStorage = null;
        viewId = -1;
    }

    @Override
    public void deleteIndex() {
        if (getViewId() <= 0) {
            return;
        }

        boolean success = false;
        try {
            dbStorage.beginTransaction();

            String[] whereArgs = {Integer.toString(getViewId())};
            dbStorage.getDatabase().delete("maps", "view_id=?", whereArgs);

            ContentValues updateValues = new ContentValues();
            updateValues.put("lastSequence", 0);
            dbStorage.getDatabase().update("views", updateValues, "view_id=?", whereArgs);

            success = true;
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error removing index", e);
        } finally {
            dbStorage.endTransaction(success);
        }
    }

    @Override
    public void deleteView() {
        //TODO: Implement
    }

    /**
     * Updates the version of the view. A change in version means the delegate's map block has
     * changed its semantics, so the index should be deleted.
     */
    @Override
    public boolean setVersion(String version) {
        // Update the version column in the db. This is a little weird looking because we want to
        // avoid modifying the db if the version didn't change, and because the row might not exist
        // yet.

        String sql = "SELECT name, version FROM views WHERE name=?";
        String[] args = {name};
        Cursor cursor = null;
        try {
            cursor = dbStorage.getDatabase().rawQuery(sql, args);
            if (!cursor.moveToNext()) {
                // no such record, so insert
                ContentValues insertValues = new ContentValues();
                insertValues.put("name", name);
                insertValues.put("version", version);
                dbStorage.getDatabase().insert("views", null, insertValues);
                return true;
            }
            ContentValues updateValues = new ContentValues();
            updateValues.put("version", version);
            updateValues.put("lastSequence", 0);
            String[] whereArgs = {name, version};
            int rowsAffected = dbStorage.getDatabase().update("views", updateValues, "name=? AND version!=?",
                    whereArgs);
            return (rowsAffected > 0);
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error setting map block", e);
            return false;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public int getTotalRows() {

        int totalRows = -1;
        String sql = "SELECT total_docs FROM views WHERE view_id=?";
        String[] args = {String.valueOf(viewId)};
        Cursor cursor = null;
        try {
            cursor = dbStorage.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                totalRows = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error getting total_docs", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // need to count & update rows
        if (totalRows < 0) {
            totalRows = countTotalRows();
            if (totalRows >= 0) {
                updateTotalRows(totalRows);
            }
        }
        return totalRows;
    }

    /**
     * The last sequence number that has been indexed.
     */
    @Override
    public long getLastSequenceIndexed() {
        String sql = "SELECT lastSequence FROM views WHERE name=?";
        String[] args = {name};
        Cursor cursor = null;
        long result = -1;
        try {
            cursor = dbStorage.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0);
            }
        } catch (Exception e) {
            Log.e(Log.TAG_VIEW, "Error getting last sequence indexed", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    @Override
    public long getLastSequenceChangedAt() {
        //TODO: Implement
        return 0;
    }

    /**
     * Updates the view's index (incrementally) if necessary.
     *
     * @return 200 if updated, 304 if already up-to-date, else an error code
     */
    @Override
    @InterfaceAudience.Private
    public void updateIndex() throws CouchbaseLiteException {
        Log.v(Log.TAG_VIEW, "Re-indexing view: %s", name);
        assert (delegate.getMap() != null);

        if (getViewId() <= 0) {
            String msg = String.format("getViewId() < 0");
            throw new CouchbaseLiteException(msg, new Status(Status.NOT_FOUND));
        }

        dbStorage.beginTransaction();
        Status result = new Status(Status.INTERNAL_SERVER_ERROR);
        Cursor cursor = null;

        try {
            long last = getLastSequenceIndexed();
            long dbMaxSequence = dbStorage.getLastSequenceNumber();
            long minLastSequence = dbMaxSequence;

            // First remove obsolete emitted results from the 'maps' table:
            if (last < 0) {
                String msg = String.format("last < 0 (%s)", last);
                throw new CouchbaseLiteException(msg, new Status(Status.INTERNAL_SERVER_ERROR));
            } else if (last < dbMaxSequence) {
                minLastSequence = Math.min(minLastSequence, last);

                if (last == 0) {

                    // If the lastSequence has been reset to 0, make sure to remove
                    // any leftover rows:
                    String[] whereArgs = {Integer.toString(getViewId())};
                    dbStorage.getDatabase().delete("maps", "view_id=?", whereArgs);
                } else {
                    dbStorage.optimizeSQLIndexes();
                    // Delete all obsolete map results (ones from since-replaced
                    // revisions):
                    String[] args = {Integer.toString(getViewId()),
                            Long.toString(last),
                            Long.toString(last)};
                    dbStorage.getDatabase().execSQL(
                            "DELETE FROM maps WHERE view_id=? AND sequence IN ("
                                    + "SELECT parent FROM revs WHERE sequence>? "
                                    + "AND +parent>0 AND +parent<=?)", args);
                }
            }

            if (minLastSequence == dbMaxSequence) {
                // nothing to do (eg,  kCBLStatusNotModified)
                Log.v(Log.TAG_VIEW, "minLastSequence (%s) == dbMaxSequence (%s), nothing to do", minLastSequence, dbMaxSequence);
                result.setCode(Status.NOT_MODIFIED);
                return;
            }

            // This is the emit() block, which gets called from within the
            // user-defined map() block
            // that's called down below.
            AbstractMapEmitBlock emitBlock = new AbstractMapEmitBlock() {
                @Override
                public void emit(Object key, Object value) {
                    try {
                        String valueJson;
                        String keyJson = Manager.getObjectMapper().writeValueAsString(key);
                        if (value == null) {
                            valueJson = null;
                        } else {
                            valueJson = Manager.getObjectMapper().writeValueAsString(value);
                        }

                        // NOTE: execSQL() is little faster than insert()
                        String[] args = {Integer.toString(getViewId()), Long.toString(sequence), keyJson, valueJson};
                        dbStorage.getDatabase().execSQL("INSERT INTO maps (view_id, sequence, key, value) VALUES(?,?,?,?) ", args);
                    } catch (Exception e) {
                        Log.e(Log.TAG_VIEW, "Error emitting", e);
                        // find a better way to propagate this back
                    }
                }
            };

            // Now scan every revision added since the last time the view was indexed:

            // NOTE: Below is original Query. In case query result uses a lot of memory,
            //       Android SQLiteDatabase causes null value column. Then it causes the missing
            //       index data because following logic skip result if column is null.
            //       To avoid the issue, retrieving json field is isolated from original query.
            //       Because json field could be large, maximum size is 2MB.
            // StringBuffer sql = new StringBuffer( "SELECT revs.doc_id, sequence, docid, revid, json, no_attachments, deleted FROM revs, docs WHERE sequence>? AND current!=0 ");

            StringBuffer sql = new StringBuffer("SELECT revs.doc_id, sequence, docid, revid, no_attachments, deleted FROM revs, docs WHERE sequence>? AND current!=0 ");
            if (minLastSequence == 0) {
                sql.append("AND deleted=0 ");
            }
            sql.append("AND revs.doc_id = docs.doc_id ORDER BY revs.doc_id, revid DESC");
            String[] selectArgs = {Long.toString(minLastSequence)};
            cursor = dbStorage.getDatabase().rawQuery(sql.toString(), selectArgs);

            boolean keepGoing = cursor.moveToNext();
            while (keepGoing) {

                // NOTE: skip row if 1st column is null
                // https://github.com/couchbase/couchbase-lite-java-core/issues/497
                if (cursor.isNull(0)) {
                    keepGoing = cursor.moveToNext();
                    continue;
                }

                long docID = cursor.getLong(0);

                // Reconstitute the document as a dictionary:
                long sequence = cursor.getLong(1);
                String docId = cursor.getString(2);
                if (docId.startsWith("_design/")) {  // design docs don't get indexed!
                    keepGoing = cursor.moveToNext();
                    continue;
                }
                String revId = cursor.getString(3);

                boolean noAttachments = cursor.getInt(4) > 0;
                boolean deleted = cursor.getInt(5) > 0;

                while ((keepGoing = cursor.moveToNext()) && (cursor.isNull(0) || cursor.getLong(0) == docID)) {
                    // Skip rows with the same doc_id -- these are losing conflicts.
                    // NOTE: Or Skip rows if 1st column is null
                    // https://github.com/couchbase/couchbase-lite-java-core/issues/497
                }

                if (minLastSequence > 0) {
                    // Find conflicts with documents from previous indexings.
                    String[] selectArgs2 = {Long.toString(docID), Long.toString(minLastSequence)};

                    Cursor cursor2 = null;
                    try {
                        cursor2 = dbStorage.getDatabase().rawQuery(
                                "SELECT revid, sequence FROM revs "
                                        + "WHERE doc_id=? AND sequence<=? AND current!=0 AND deleted=0 "
                                        + "ORDER BY revID DESC "
                                        + "LIMIT 1", selectArgs2);

                        if (cursor2.moveToNext()) {
                            String oldRevId = cursor2.getString(0);
                            // This is the revision that used to be the 'winner'.
                            // Remove its emitted rows:
                            long oldSequence = cursor2.getLong(1);
                            String[] args = {
                                    Integer.toString(getViewId()),
                                    Long.toString(oldSequence)
                            };
                            dbStorage.getDatabase().execSQL(
                                    "DELETE FROM maps WHERE view_id=? AND sequence=?", args);
                            if (deleted || RevisionInternal.CBLCompareRevIDs(oldRevId, revId) > 0) {
                                // It still 'wins' the conflict, so it's the one that
                                // should be mapped [again], not the current revision!
                                revId = oldRevId;
                                sequence = oldSequence;
                                deleted = false;
                            }
                        }
                    } finally {
                        if (cursor2 != null) {
                            cursor2.close();
                        }
                    }
                }

                if (deleted) {
                    continue;
                }

                String[] selectArgs3 = {Long.toString(sequence)};
                byte[] json = Utils.byteArrayResultForQuery(dbStorage.getDatabase(), "SELECT json FROM revs WHERE sequence=?", selectArgs3);

                // Get the document properties, to pass to the map function:
                EnumSet<Database.TDContentOptions> contentOptions = EnumSet.noneOf(Database.TDContentOptions.class);
                if (noAttachments)
                    contentOptions.add(Database.TDContentOptions.TDNoAttachments);
                Map<String, Object> properties = dbStorage.documentPropertiesFromJSON(
                        json,
                        docId,
                        revId,
                        false,
                        sequence,
                        contentOptions
                );
                if (properties != null) {
                    // Call the user-defined map() to emit new key/value
                    // pairs from this revision:
                    emitBlock.setSequence(sequence);
                    delegate.getMap().map(properties, emitBlock);

                    properties.clear();
                }
            }

            // Finally, record the last revision sequence number that was
            // indexed:
            ContentValues updateValues = new ContentValues();
            updateValues.put("lastSequence", dbMaxSequence);
            updateValues.put("total_docs", countTotalRows());
            String[] whereArgs = {Integer.toString(getViewId())};
            dbStorage.getDatabase().update("views", updateValues, "view_id=?", whereArgs);

            // FIXME actually count number added :)
            Log.v(Log.TAG_VIEW, "Finished re-indexing view: %s " + " up to sequence %s", name, dbMaxSequence);
            result.setCode(Status.OK);
        } catch (SQLException e) {
            throw new CouchbaseLiteException(e, new Status(Status.DB_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            if (!result.isSuccessful()) {
                Log.w(Log.TAG_VIEW, "Failed to rebuild view %s.  Result code: %d", name, result.getCode());
            }
            if (dbStorage != null) {
                dbStorage.endTransaction(result.isSuccessful());
            }
        }
    }

    /**
     * Queries the view without performing any reducing or grouping.
     */
    @Override
    public List<QueryRow> regularQuery(QueryOptions options){
        //TODO: Implement
        return null;
    }
    /**
     *  Queries the view, with reducing or grouping as per the options.
     */
    @Override
    public List<QueryRow> reducedQuery(QueryOptions options){
        //TODO: Implement
        return null;
    }

    @Override
    public int getViewId() {
        if (viewId < 0) {
            String sql = "SELECT view_id FROM views WHERE name=?";
            String[] args = {name};
            Cursor cursor = null;
            try {
                cursor = dbStorage.getDatabase().rawQuery(sql, args);
                if (cursor.moveToNext()) {
                    viewId = cursor.getInt(0);
                }
            } catch (SQLException e) {
                Log.e(Log.TAG_VIEW, "Error getting view id", e);
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }
        return viewId;
    }

    @Override
    public int countTotalRows() {
        int totalRows = -1;
        String sql = "SELECT COUNT(view_id) FROM maps WHERE view_id=?";
        String[] args = {String.valueOf(viewId)};
        Cursor cursor = null;
        try {
            cursor = dbStorage.getDatabase().rawQuery(sql, args);
            if (cursor.moveToNext()) {
                totalRows = cursor.getInt(0);
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error getting total_docs", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return totalRows;
    }

    @Override
    public void updateTotalRows(int totalRows) {
        ContentValues values = new ContentValues();
        values.put("total_docs=", totalRows);
        dbStorage.getDatabase().update("views", values, "view_id=?", new String[]{String.valueOf(viewId)});
    }

    @Override
    public List<QueryRow> queryWithOptions(QueryOptions options) throws CouchbaseLiteException {

        if (options == null) {
            options = new QueryOptions();
        }

        Cursor cursor = null;
        List<QueryRow> rows = new ArrayList<QueryRow>();
        Predicate<QueryRow> postFilter = options.getPostFilter();

        try {
            cursor = resultSetWithOptions(options);
            int groupLevel = options.getGroupLevel();
            boolean group = options.isGroup() || (groupLevel > 0);
            boolean reduce = options.isReduce() || group;

            if (reduce && (delegate.getReduce() == null) && !group) {
                Log.w(Log.TAG_VIEW, "Cannot use reduce option in view %s which has no reduce block defined", name);
                throw new CouchbaseLiteException(new Status(Status.BAD_REQUEST));
            }

            if (reduce || group) {
                // Reduced or grouped query:
                rows = reducedQuery(cursor, group, groupLevel, postFilter);
            } else {
                // regular query
                cursor.moveToNext();
                while (!cursor.isAfterLast()) {
                    JsonDocument keyDoc = new JsonDocument(cursor.getBlob(0));
                    JsonDocument valueDoc = new JsonDocument(cursor.getBlob(1));
                    String docId = cursor.getString(2);
                    int sequence = Integer.valueOf(cursor.getString(3));
                    Map<String, Object> docContents = null;
                    if (options.isIncludeDocs()) {
                        Object valueObject = valueDoc.jsonObject();
                        // http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Linked_documents
                        if (valueObject instanceof Map && ((Map) valueObject).containsKey("_id")) {
                            String linkedDocId = (String) ((Map) valueObject).get("_id");
                            RevisionInternal linkedDoc = dbStorage.getDocumentWithIDAndRev(
                                    linkedDocId,
                                    null,
                                    EnumSet.noneOf(Database.TDContentOptions.class)
                            );
                            if (linkedDoc != null) {
                                docContents = linkedDoc.getProperties();
                            }
                        } else {
                            docContents = dbStorage.documentPropertiesFromJSON(
                                    cursor.getBlob(5),
                                    docId,
                                    cursor.getString(4),
                                    false,
                                    cursor.getLong(3),
                                    options.getContentOptions()
                            );
                        }
                    }
                    QueryRow row = new QueryRow(docId, sequence, keyDoc.jsonObject(), valueDoc.jsonObject(), docContents);
                    row.setDatabase(db);
                    if (postFilter == null || postFilter.apply(row)) {
                        rows.add(row);
                    }
                    cursor.moveToNext();
                }
            }

        } catch (SQLException e) {
            String errMsg = String.format("Error querying view: %s", this);
            Log.e(Log.TAG_VIEW, errMsg, e);
            throw new CouchbaseLiteException(errMsg, e, new Status(Status.DB_ERROR));
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        return rows;
    }

    @Override
    public List<Map<String, Object>> dump() {
        if (getViewId() < 0) {
            return null;
        }

        String[] selectArgs = {Integer.toString(getViewId())};
        Cursor cursor = null;
        List<Map<String, Object>> result = null;

        try {
            cursor = dbStorage
                    .getDatabase()
                    .rawQuery(
                            "SELECT sequence, key, value FROM maps WHERE view_id=? ORDER BY key",
                            selectArgs);

            cursor.moveToNext();
            result = new ArrayList<Map<String, Object>>();
            while (!cursor.isAfterLast()) {
                Map<String, Object> row = new HashMap<String, Object>();
                row.put("seq", cursor.getInt(0));
                row.put("key", cursor.getString(1));
                row.put("value", cursor.getString(2));
                result.add(row);
                cursor.moveToNext();
            }
        } catch (SQLException e) {
            Log.e(Log.TAG_VIEW, "Error dumping view", e);
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }
    ///////////////////////////////////////////////////////////////////////////
    // Internal Instance Methods
    ///////////////////////////////////////////////////////////////////////////

    private Cursor resultSetWithOptions(QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }

        // OPT: It would be faster to use separate tables for raw-or ascii-collated views so that
        // they could be indexed with the right collation, instead of having to specify it here.
        String collationStr = "";
        if (collation == View.TDViewCollation.TDViewCollationASCII) {
            collationStr += " COLLATE JSON_ASCII";
        } else if (collation == View.TDViewCollation.TDViewCollationRaw) {
            collationStr += " COLLATE JSON_RAW";
        }

        String sql = "SELECT key, value, docid, revs.sequence";
        if (options.isIncludeDocs()) {
            sql = sql + ", revid, json";
        }
        sql = sql + " FROM maps, revs, docs WHERE maps.view_id=?";

        List<String> argsList = new ArrayList<String>();
        argsList.add(Integer.toString(getViewId()));

        if (options.getKeys() != null) {
            sql += " AND key in (";
            String item = "?";
            for (Object key : options.getKeys()) {
                sql += item;
                item = ", ?";
                argsList.add(toJSONString(key));
            }
            sql += ")";
        }

        Object minKey = options.getStartKey();
        Object maxKey = options.getEndKey();
        String minKeyDocId = options.getStartKeyDocId();
        String maxKeyDocId = options.getEndKeyDocId();

        boolean inclusiveMin = true;
        boolean inclusiveMax = options.isInclusiveEnd();
        if (options.isDescending()) {
            Object min = minKey;
            minKey = maxKey;
            maxKey = min;
            inclusiveMin = inclusiveMax;
            inclusiveMax = true;
            minKeyDocId = options.getEndKeyDocId();
            maxKeyDocId = options.getStartKeyDocId();
        }

        if (minKey != null) {
            if (inclusiveMin) {
                sql += " AND key >= ?";
            } else {
                sql += " AND key > ?";
            }
            sql += collationStr;
            String minKeyJSON = toJSONString(minKey);
            argsList.add(minKeyJSON);
            if (minKeyDocId != null && inclusiveMin) {
                //OPT: This calls the JSON collator a 2nd time unnecessarily.
                sql += String.format(" AND (key > ? %s OR docid >= ?)", collationStr);
                argsList.add(minKeyJSON);
                argsList.add(minKeyDocId);
            }
        }

        if (maxKey != null) {
            maxKey = View.keyForPrefixMatch(maxKey, options.getPrefixMatchLevel());
            if (inclusiveMax) {
                sql += " AND key <= ?";
            } else {
                sql += " AND key < ?";
            }
            sql += collationStr;
            String maxKeyJSON = toJSONString(maxKey);
            argsList.add(maxKeyJSON);
            if (maxKeyDocId != null && inclusiveMax) {
                sql += String.format(" AND (key < ? %s OR docid <= ?)", collationStr);
                argsList.add(maxKeyJSON);
                argsList.add(maxKeyDocId);
            }
        }

        sql = sql
                + " AND revs.sequence = maps.sequence AND docs.doc_id = revs.doc_id ORDER BY key";
        sql += collationStr;

        if (options.isDescending()) {
            sql = sql + " DESC";
        }

        sql = sql + " LIMIT ? OFFSET ?";
        argsList.add(Integer.toString(options.getLimit()));
        argsList.add(Integer.toString(options.getSkip()));

        Log.v(Log.TAG_VIEW, "Query %s: %s | args: %s", name, sql, argsList);

        Cursor cursor = dbStorage.getDatabase().rawQuery(sql,
                argsList.toArray(new String[argsList.size()]));
        return cursor;
    }

    private String toJSONString(Object object) {
        if (object == null) {
            return null;
        }
        String result = null;
        try {
            result = Manager.getObjectMapper().writeValueAsString(object);
        } catch (Exception e) {
            Log.w(Log.TAG_VIEW, "Exception serializing object to json: %s", e, object);
        }
        return result;
    }

    private List<QueryRow> reducedQuery(Cursor cursor,
                                        boolean group,
                                        int groupLevel,
                                        Predicate<QueryRow> postFilter)
            throws CouchbaseLiteException {

        List<Object> keysToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        List<Object> valuesToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        Object lastKey = null;
        List<QueryRow> rows = new ArrayList<QueryRow>();

        cursor.moveToNext();
        while (!cursor.isAfterLast()) {
            JsonDocument keyDoc = new JsonDocument(cursor.getBlob(0));
            JsonDocument valueDoc = new JsonDocument(cursor.getBlob(1));
            assert (keyDoc != null);

            Object keyObject = keyDoc.jsonObject();
            if (group && !groupTogether(keyObject, lastKey, groupLevel)) {
                if (lastKey != null) {
                    // This pair starts a new group, so reduce & record the last one:
                    Object reduced = (delegate.getReduce() != null) ? delegate.getReduce().reduce(keysToReduce, valuesToReduce, false) : null;
                    Object key = View.groupKey(lastKey, groupLevel);
                    QueryRow row = new QueryRow(null, 0, key, reduced, null);
                    row.setDatabase(db);
                    if (postFilter == null || postFilter.apply(row)) {
                        rows.add(row);
                    }
                    keysToReduce.clear();
                    valuesToReduce.clear();
                }
                lastKey = keyObject;
            }
            if (keysToReduce == null) {
                //with group, no reduce
                keysToReduce = new ArrayList<Object>();
            }
            if (valuesToReduce == null) {
                //with group, no reduce
                valuesToReduce = new ArrayList<Object>();
            }
            keysToReduce.add(keyObject);
            valuesToReduce.add(valueDoc.jsonObject());
            cursor.moveToNext();
        }

        if (keysToReduce != null && keysToReduce.size() > 0) {
            // Finish the last group (or the entire list, if no grouping):
            Object key = group ? View.groupKey(lastKey, groupLevel) : null;
            Object reduced = (delegate.getReduce() != null) ? delegate.getReduce().reduce(keysToReduce, valuesToReduce, false) : null;
            QueryRow row = new QueryRow(null, 0, key, reduced, null);
            row.setDatabase(db);
            if (postFilter == null || postFilter.apply(row)) {
                rows.add(row);
            }
        }
        return rows;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal Static Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Are key1 and key2 grouped together at this groupLevel?
     */
    private static boolean groupTogether(Object key1, Object key2, int groupLevel) {
        if (groupLevel == 0 || !(key1 instanceof List) || !(key2 instanceof List)) {
            return key1.equals(key2);
        }
        @SuppressWarnings("unchecked")
        List<Object> key1List = (List<Object>) key1;
        @SuppressWarnings("unchecked")
        List<Object> key2List = (List<Object>) key2;

        // if either key list is smaller than groupLevel and the key lists are different
        // sizes, they cannot be equal.
        if ((key1List.size() < groupLevel || key2List.size() < groupLevel) && key1List.size() != key2List.size()) {
            return false;
        }

        int end = Math.min(groupLevel, Math.min(key1List.size(), key2List.size()));
        for (int i = 0; i < end; ++i) {
            if (!key1List.get(i).equals(key2List.get(i))) {
                return false;
            }
        }
        return true;
    }
}
