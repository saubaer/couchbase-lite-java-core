//
//  Storage.java
//
//  Created by Hideki Itakura on 6/10/15.
//  Copyright (c) 2015 Couchbase, Inc All rights reserved.
//
package com.couchbase.lite;

import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.storage.SQLiteStorageEngine;

import java.net.URL;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hideki on 6/16/15.
 */
public interface Storage {
    // temporary
    SQLiteStorageEngine getDatabase();
    boolean beginTransaction();
    boolean endTransaction(boolean commit);
    void optimizeSQLIndexes();
    String publicUUID();
    String privateUUID();
    RevisionInternal getDocumentWithIDAndRev(String id, String rev, EnumSet<Database.TDContentOptions> contentOptions);
    Map<String, Object> getAttachmentsDictForSequenceWithContent(long sequence, EnumSet<Database.TDContentOptions> contentOptions);
    List<RevisionInternal> getRevisionHistory(RevisionInternal rev);
    RevisionList getAllRevisionsOfDocumentID(String docId, long docNumericID, boolean onlyCurrent);
    RevisionList getAllRevisionsOfDocumentID(String docId, boolean onlyCurrent);
    long getDocNumericID(String docId);
    int pruneRevsToMaxDepth(int maxDepth) throws CouchbaseLiteException;
    Status garbageCollectAttachments();
    RevisionInternal loadRevisionBody(RevisionInternal rev, EnumSet<Database.TDContentOptions> contentOptions) throws CouchbaseLiteException;
    List<String>  getPossibleAncestorRevisionIDs( RevisionInternal rev, int limit, AtomicBoolean hasAttachment );
    void forceInsert(RevisionInternal rev, List<String> revHistory, URL source) throws CouchbaseLiteException;
    String winningRevIDOfDoc(long docNumericId, AtomicBoolean outIsDeleted, AtomicBoolean outIsConflict) throws CouchbaseLiteException;
    RevisionInternal putRevision(RevisionInternal oldRev, String prevRevId, boolean allowConflict, Status resultStatus) throws CouchbaseLiteException;
    boolean existsDocumentWithIDAndRev(String docId, String revId);
    String findCommonAncestorOf(RevisionInternal rev, List<String> revIDs);
    int findMissingRevisions(RevisionList touchRevs);
    RevisionList changesSince(long lastSeq, ChangesOptions options, ReplicationFilter filter, Map<String, Object> filterParams);
    List<View> getAllViews();
    void copyAttachmentNamedFromSequenceToSequence(String name, long fromSeq, long toSeq) throws CouchbaseLiteException;
    String lastSequenceWithCheckpointId(String checkpointId);
    RevisionInternal getLocalDocument(String docID, String revID);
    RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID) throws CouchbaseLiteException;
    void deleteLocalDocument(String docID, String revID) throws CouchbaseLiteException;
    boolean replaceUUIDs();
    Status deleteViewNamed(String name);
    Map<String,Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException;
    Map<String, Object> documentPropertiesFromJSON(byte[] json, String docId, String revId, boolean deleted, long sequence, EnumSet<Database.TDContentOptions> contentOptions);
    Attachment getAttachmentForSequence(long sequence, String filename) throws CouchbaseLiteException;
    String getAttachmentPathForSequence(long sequence, String filename) throws CouchbaseLiteException;
    Map<String, Object> purgeRevisions(final Map<String, List<String>> docsToRevs);
    boolean setLastSequence(String lastSequence, String checkpointId, boolean push);
    RevisionInternal getParentRevision(RevisionInternal rev);

    // official
    boolean open();

    /**
     * Closes storage before it's deallocated.
     */
    void close();

    /**
     * The delegate object, which in practice is the CBLDatabase.
     */
    void setDelegate(StorageDelegate delegate);

    StorageDelegate getDelegate();

    /**
     * Is a transaction active?
     */
    boolean inTransaction();

    boolean runInTransaction(TransactionalTask task);

    int getDocumentCount();

    void setMaxRevTreeDepth(int maxRevTreeDepth);
    int getMaxRevTreeDepth();

    String infoForKey(String key);

    long setInfo(String key, String info);

    long getLastSequenceNumber();

    void insertAttachmentForSequenceWithNameAndType(long sequence, String name, String contentType, int revpos, BlobKey key, long length, AttachmentInternal.AttachmentEncoding encoding, long encodedLength) throws CouchbaseLiteException;

    void compact() throws CouchbaseLiteException;
}
