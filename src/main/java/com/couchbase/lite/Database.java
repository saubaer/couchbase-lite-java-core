/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;

import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.Body;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.replicator.Replication;
import com.couchbase.lite.replicator.ReplicationState;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;
import com.couchbase.lite.store.SQLiteStore;
import com.couchbase.lite.store.Store;
import com.couchbase.lite.store.StoreDelegate;
import com.couchbase.lite.support.Base64;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.support.HttpClientFactory;
import com.couchbase.lite.support.PersistentCookieStore;
import com.couchbase.lite.support.RevisionUtils;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.StreamUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A CouchbaseLite Database.
 */
public final class Database implements StoreDelegate {

    /**
     * Options for what metadata to include in document bodies
     */
    public enum TDContentOptions {
        TDIncludeAttachments, TDIncludeConflicts, TDIncludeRevs, TDIncludeRevsInfo, TDIncludeLocalSeq, TDNoBody, TDBigAttachmentsFollow, TDNoAttachments
    }

    public static final String TAG = Log.TAG_DATABASE;
    // When this many changes pile up in _changesToNotify, start removing their bodies to save RAM
    private static final int MANY_CHANGES_TO_NOTIFY = 5000;

    private static ReplicationFilterCompiler filterCompiler;
    /**
     * Length that constitutes a 'big' attachment
     */
    public static int kBigAttachmentLength = (16 * 1024);

    // Default value for maxRevTreeDepth, the max rev depth to preserve in a prune operation
    public static int DEFAULT_MAX_REVS = 20;


    private Store store = null;
    private String path;
    private String name;

    private boolean open = false;

    private Map<String, View> views;
    private Map<String, ReplicationFilter> filters;
    private Map<String, Validator> validations;

    private Map<String, BlobStoreWriter> pendingAttachmentsByDigest;
    private Set<Replication> activeReplicators;
    private Set<Replication> allReplicators;

    private BlobStore attachments;
    private Manager manager;
    final private CopyOnWriteArrayList<ChangeListener> changeListeners;
    final private CopyOnWriteArrayList<DatabaseListener> databaseListeners;
    private Cache<String, Document> docCache;
    private List<DocumentChange> changesToNotify;
    private boolean postingChangeNotifications;
    private long startTime;

    /**
     * Each database can have an associated PersistentCookieStore,
     * where the persistent cookie store uses the database to store
     * its cookies.
     * <p/>
     * There are two reasons this has been made an instance variable
     * of the Database, rather than of the Replication:
     * <p/>
     * - The PersistentCookieStore needs to span multiple replications.
     * For example, if there is a "push" and a "pull" replication for
     * the same DB, they should share a cookie store.
     * <p/>
     * - PersistentCookieStore lifecycle should be tied to the Database
     * lifecycle, since it needs to cease to exist if the underlying
     * Database ceases to exist.
     * <p/>
     * REF: https://github.com/couchbase/couchbase-lite-android/issues/269
     */
    private PersistentCookieStore persistentCookieStore;


    /**
     * Constructor
     */
    @InterfaceAudience.Private
    public Database(String path, Manager manager) {
        assert (new File(path).isAbsolute()); //path must be absolute
        this.path = path;
        this.name = FileDirUtils.getDatabaseNameFromPath(path);
        this.manager = manager;
        this.changeListeners = new CopyOnWriteArrayList<ChangeListener>();
        this.databaseListeners = new CopyOnWriteArrayList<DatabaseListener>();
        this.docCache = new Cache<String, Document>();
        this.startTime = System.currentTimeMillis();
        this.changesToNotify = new ArrayList<DocumentChange>();
        this.activeReplicators = Collections.synchronizedSet(new HashSet());
        this.allReplicators = Collections.synchronizedSet(new HashSet());
    }


    ///////////////////////////////////////////////////////////////////////////
    // APIs
    // https://github.com/couchbaselabs/couchbase-lite-api/blob/master/gen/md/Database.md
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Class Members - Properties
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Returns the currently registered filter compiler (nil by default).
     */
    @InterfaceAudience.Public
    public static ReplicationFilterCompiler getFilterCompiler() {
        return filterCompiler;
    }

    /**
     * Registers an object that can compile source code into executable filter blocks.
     */
    @InterfaceAudience.Public
    public static void setFilterCompiler(ReplicationFilterCompiler filterCompiler) {
        Database.filterCompiler = filterCompiler;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Members - Properties
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Get all the replicators associated with this database.
     */
    @InterfaceAudience.Public
    public List<Replication> getAllReplications() {
        List<Replication> allReplicatorsList = new ArrayList<Replication>();
        if (allReplicators != null) {
            allReplicatorsList.addAll(allReplicators);
        }
        return allReplicatorsList;
    }

    /**
     * The number of documents in the database.
     */
    @InterfaceAudience.Public
    public int getDocumentCount() {
        return store == null ? 0 : store.getDocumentCount();
    }

    /**
     * The latest sequence number used.  Every new revision is assigned a new sequence number,
     * so this property increases monotonically as changes are made to the database. It can be
     * used to check whether the database has changed between two points in time.
     */
    @InterfaceAudience.Public
    public long getLastSequenceNumber() {
        return store == null ? 0 : store.getLastSequence();
    }

    /**
     * The database manager that owns this database.
     */
    @InterfaceAudience.Public
    public Manager getManager() {
        return manager;
    }

    /**
     * Get the database's name.
     */
    @InterfaceAudience.Public
    public String getName() {
        return name;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Instance Members - Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Adds a Database change delegate that will be called whenever a Document within the Database changes.
     */
    @InterfaceAudience.Public
    public void addChangeListener(ChangeListener listener) {
        changeListeners.addIfAbsent(listener);
    }

    /**
     * Compacts the database file by purging non-current JSON bodies, pruning revisions older than
     * the maxRevTreeDepth, deleting unused attachment files, and vacuuming the SQLite database.
     */
    @InterfaceAudience.Public
    public void compact() throws CouchbaseLiteException {
        if (store != null) {
            store.compact();
            store.garbageCollectAttachments();
        }
    }

    /**
     * Returns a query that matches all documents in the database.
     * This is like querying an imaginary view that emits every document's ID as a key.
     */
    @InterfaceAudience.Public
    public Query createAllDocumentsQuery() {
        return new Query(this, (View) null);
    }

    /**
     * Creates a new Document object with no properties and a new (random) UUID.
     * The document will be saved to the database when you call -createRevision: on it.
     */
    @InterfaceAudience.Public
    public Document createDocument() {
        return getDocument(Misc.CreateUUID());
    }

    /**
     * Creates a new Replication that will pull from the source Database at the given url.
     *
     * @param remote the remote URL to pull from
     * @return A new Replication that will pull from the source Database at the given url.
     */
    @InterfaceAudience.Public
    public Replication createPullReplication(URL remote) {
        return new Replication(this, remote, Replication.Direction.PULL, null, manager.getWorkExecutor());

    }

    /**
     * Creates a new Replication that will push to the target Database at the given url.
     *
     * @param remote the remote URL to push to
     * @return A new Replication that will push to the target Database at the given url.
     */
    @InterfaceAudience.Public
    public Replication createPushReplication(URL remote) {
        return new Replication(this, remote, Replication.Direction.PUSH, null, manager.getWorkExecutor());
    }

    /**
     * Deletes the database.
     */
    @InterfaceAudience.Public
    public void delete() throws CouchbaseLiteException {
        if (open) {
            if (!close()) {
                throw new CouchbaseLiteException("The database was open, and could not be closed", Status.INTERNAL_SERVER_ERROR);
            }
        }
        manager.forgetDatabase(this);
        if (!exists()) {
            return;
        }
        File file = new File(path);
        File fileJournal = new File(path + "-journal");

        boolean deleteStatus = file.delete();
        if (fileJournal.exists()) {
            deleteStatus &= fileJournal.delete();
        }

        File attachmentsFile = new File(getAttachmentStorePath());

        //recursively delete attachments path
        boolean deleteAttachmentStatus = FileDirUtils.deleteRecursive(attachmentsFile);

        if (!deleteStatus) {
            throw new CouchbaseLiteException("Was not able to delete the database file", Status.INTERNAL_SERVER_ERROR);
        }

        if (!deleteAttachmentStatus) {
            throw new CouchbaseLiteException("Was not able to delete the attachments files", Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Deletes the local document with the given ID.
     */
    @InterfaceAudience.Public
    public boolean deleteLocalDocument(String id) throws CouchbaseLiteException {
        id = makeLocalDocumentId(id);
        RevisionInternal prevRev = getLocalDocument(id, null);
        if (prevRev == null) {
            return false;
        }
        store.deleteLocalDocument(id, prevRev.getRevId());
        return true;
    }

    /**
     * Instantiates a Document object with the given ID.
     * Doesn't touch the on-disk sqliteDb; a document with that ID doesn't
     * even need to exist yet. CBLDocuments are cached, so there will
     * never be more than one instance (in this sqliteDb) at a time with
     * the same documentID.
     * <p/>
     * NOTE: the caching described above is not implemented yet
     */
    @InterfaceAudience.Public
    public Document getDocument(String documentId) {
        if (documentId == null || documentId.length() == 0) {
            return null;
        }
        Document doc = docCache.get(documentId);
        if (doc == null) {
            doc = new Document(this, documentId);
            if (doc == null) {
                return null;
            }
            docCache.put(documentId, doc);
        }
        return doc;
    }

    /**
     * Gets the Document with the given id, or null if it does not exist.
     */
    @InterfaceAudience.Public
    public Document getExistingDocument(String documentId) {
        if (documentId == null || documentId.length() == 0) {
            return null;
        }
        RevisionInternal revisionInternal = getDocument(documentId, null, EnumSet.noneOf(Database.TDContentOptions.class));
        if (revisionInternal == null) {
            return null;
        }
        return getDocument(documentId);
    }

    /**
     * Returns the contents of the local document with the given ID, or nil if none exists.
     */
    @InterfaceAudience.Public
    public Map<String, Object> getExistingLocalDocument(String documentId) {
        RevisionInternal revInt = getLocalDocument(makeLocalDocumentId(documentId), null);
        if (revInt == null) {
            return null;
        }
        return revInt.getProperties();
    }

    /**
     * Returns the existing View with the given name, or nil if none.
     */
    @InterfaceAudience.Public
    public View getExistingView(String name) {
        View view = null;
        if (views != null) {
            view = views.get(name);
        }
        if (view != null) {
            return view;
        }

        //view is not in cache but it maybe in DB
        view = new View(this, name);
        if (view.getViewId() > 0) {
            return view;
        }

        return null;
    }

    /**
     * Returns the existing filter function (block) registered with the given name.
     * Note that filters are not persistent -- you have to re-register them on every launch.
     */
    @InterfaceAudience.Public
    public ReplicationFilter getFilter(String filterName) {
        ReplicationFilter result = null;
        if (filters != null) {
            result = filters.get(filterName);
        }
        if (result == null) {
            ReplicationFilterCompiler filterCompiler = getFilterCompiler();
            if (filterCompiler == null) {
                return null;
            }

            List<String> outLanguageList = new ArrayList<String>();
            String sourceCode = getDesignDocFunction(filterName, "filters", outLanguageList);
            if (sourceCode == null) {
                return null;
            }
            String language = outLanguageList.get(0);
            ReplicationFilter filter = filterCompiler.compileFilterFunction(sourceCode, language);
            if (filter == null) {
                Log.w(Database.TAG, "Filter %s failed to compile", filterName);
                return null;
            }
            setFilter(filterName, filter);
            return filter;
        }
        return result;
    }

    /**
     * Returns the existing document validation function (block) registered with the given name.
     * Note that validations are not persistent -- you have to re-register them on every launch.
     */
    @InterfaceAudience.Public
    public Validator getValidation(String name) {
        Validator result = null;
        if (validations != null) {
            result = validations.get(name);
        }
        return result;
    }

    /**
     * Returns a View object for the view with the given name.
     * (This succeeds even if the view doesn't already exist, but the view won't be added to
     * the database until the View is assigned a map function.)
     */
    @InterfaceAudience.Public
    public View getView(String name) {
        View view = null;
        if (views != null) {
            view = views.get(name);
        }
        if (view != null) {
            return view;
        }
        return registerView(new View(this, name));
    }

    /**
     * Sets the contents of the local document with the given ID. Unlike CouchDB, no revision-ID
     * checking is done; the put always succeeds. If the properties dictionary is nil, the document
     * will be deleted.
     */
    @InterfaceAudience.Public
    public boolean putLocalDocument(String id, Map<String, Object> properties) throws CouchbaseLiteException {
        // TODO: the iOS implementation wraps this in a transaction, this should do the same.
        id = makeLocalDocumentId(id);
        RevisionInternal prevRev = getLocalDocument(id, null);
        if (prevRev == null && properties == null) {
            return false;
        }
        boolean deleted = false;
        if (properties == null) {
            deleted = true;
        }
        RevisionInternal rev = new RevisionInternal(id, null, deleted);

        if (properties != null) {
            rev.setProperties(properties);
        }

        if (prevRev == null) {
            return putLocalRevision(rev, null) != null;
        } else {
            return putLocalRevision(rev, prevRev.getRevId()) != null;
        }
    }

    /**
     * Removes the specified delegate as a listener for the Database change event.
     */
    @InterfaceAudience.Public
    public void removeChangeListener(ChangeListener listener) {
        changeListeners.remove(listener);
    }

    /**
     * Runs the delegate asynchronously.
     */
    @InterfaceAudience.Public
    public Future runAsync(final AsyncTask asyncTask) {
        return getManager().runAsync(new Runnable() {
            @Override
            public void run() {
                asyncTask.run(Database.this);
            }
        });
    }

    /**
     * Runs the block within a transaction. If the block returns NO, the transaction is rolled back.
     * Use this when performing bulk write operations like multiple inserts/updates;
     * it saves the overhead of multiple SQLite commits, greatly improving performance.
     * <p/>
     * Does not commit the transaction if the code throws an Exception.
     * <p/>
     * TODO: the iOS version has a retry loop, so there should be one here too
     */
    @InterfaceAudience.Public
    public boolean runInTransaction(TransactionalTask task) {
        return store == null ? false : store.runInTransaction(task);
    }

    /**
     * Define or clear a named filter function.
     * <p/>
     * Filters are used by push replications to choose which documents to send.
     */
    @InterfaceAudience.Public
    public void setFilter(String filterName, ReplicationFilter filter) {
        if (filters == null) {
            filters = new HashMap<String, ReplicationFilter>();
        }
        if (filter != null) {
            filters.put(filterName, filter);
        } else {
            filters.remove(filterName);
        }
    }

    /**
     * Defines or clears a named document validation function.
     * Before any change to the database, all registered validation functions are called and given a
     * chance to reject it. (This includes incoming changes from a pull replication.)
     */
    @InterfaceAudience.Public
    public void setValidation(String name, Validator validator) {
        if (validations == null) {
            validations = new HashMap<String, Validator>();
        }
        if (validator != null) {
            validations.put(name, validator);
        } else {
            validations.remove(name);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Events
    ///////////////////////////////////////////////////////////////////////////

    /**
     * The type of event raised when a Database changes.
     */
    @InterfaceAudience.Public
    public static class ChangeEvent {
        private Database source;
        private boolean isExternal;
        private List<DocumentChange> changes;

        public ChangeEvent(Database source, boolean isExternal, List<DocumentChange> changes) {
            this.source = source;
            this.isExternal = isExternal;
            this.changes = changes;
        }

        public Database getSource() {
            return source;
        }

        public boolean isExternal() {
            return isExternal;
        }

        public List<DocumentChange> getChanges() {
            return changes;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Delegates
    ///////////////////////////////////////////////////////////////////////////

    /**
     * A delegate that can be used to listen for Database changes.
     */
    @InterfaceAudience.Public
    public interface ChangeListener {
        void changed(ChangeEvent event);
    }

    // ReplicationFilterCompiler -> ReplicationFilterCompiler.java

    // ReplicationFilter -> ReplicationFilter.java

    // AsyncTask -> AsyncTask.java

    // TransactionalTask -> TransactionalTask.java

    // Validator -> Validator.java

    ///////////////////////////////////////////////////////////////////////////
    // End of APIs
    ///////////////////////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////////////////////
    // Override Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Returns a string representation of this database.
     */
    @InterfaceAudience.Public
    public String toString() {
        return this.getClass().getName() + "[" + path + "]";
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of StorageDelegate
    ///////////////////////////////////////////////////////////////////////////

    /**
     * in CBLDatabase+Internal.m
     * - (void) storageExitedTransaction: (BOOL)committed
     */
    @InterfaceAudience.Private
    public void storageExitedTransaction(boolean committed) {
        if (!committed) {
            // I already told cached CBLDocuments about these new revisions. Back that out:
            for (DocumentChange change : changesToNotify) {
                Document doc = cachedDocumentWithID(change.getDocumentId());
                if (doc != null) {
                    doc.forgetCurrentRevision();
                }
            }
            changesToNotify.clear();
        }
        postChangeNotifications();
    }

    /**
     * in CBLDatabase+Internal.m
     * - (void) databaseStorageChanged:(CBLDatabaseChange *)change
     */
    @InterfaceAudience.Private
    public void databaseStorageChanged(DocumentChange change) {
        Log.v(Log.TAG_DATABASE, "Added: " + change.getAddedRevision());
        if (changesToNotify == null) {
            changesToNotify = new ArrayList<DocumentChange>();
        }
        changesToNotify.add(change);
        if (!postChangeNotifications()) {
            // The notification wasn't posted yet, probably because a transaction is open.
            // But the CBLDocument, if any, needs to know right away so it can update its
            // currentRevision.

            Document doc = cachedDocumentWithID(change.getDocumentId());
            if (doc != null) {
                doc.revisionAdded(change, false);
            }
        }

        // Squish the change objects if too many of them are piling up:
        if (changesToNotify.size() >= MANY_CHANGES_TO_NOTIFY) {
            if (changesToNotify.size() == MANY_CHANGES_TO_NOTIFY) {
                for (DocumentChange c : changesToNotify) {
                    c.reduceMemoryUsage();
                }
            } else {
                change.reduceMemoryUsage();
            }
        }
    }

    /**
     * Generates a revision ID for a new revision.
     *
     * @param json      The canonical JSON of the revision (with metadata properties removed.)
     * @param deleted   YES if this revision is a deletion
     * @param prevRevID The parent's revision ID, or nil if this is a new document.
     */
    @InterfaceAudience.Private
    public String generateRevID(byte[] json, boolean deleted, String prevRevID){
        // TODO
        return null;
    }

    @InterfaceAudience.Private
    public BlobStore getAttachments() {
        return attachments;
    }

    @InterfaceAudience.Private
    public Map<String, Validator> getValidations() {
        return validations;
    }

    @InterfaceAudience.Private
    public void validateRevision(RevisionInternal newRev, RevisionInternal oldRev, String parentRevID) throws CouchbaseLiteException {
        if (validations == null || validations.size() == 0) {
            return;
        }

        SavedRevision publicRev = new SavedRevision(this, newRev);
        publicRev.setParentRevisionID(parentRevID);

        ValidationContextImpl context = new ValidationContextImpl(this, oldRev, newRev);

        for (String validationName : validations.keySet()) {
            Validator validation = getValidation(validationName);
            validation.validate(publicRev, context);
            if (context.getRejectMessage() != null) {
                throw new CouchbaseLiteException(context.getRejectMessage(), Status.FORBIDDEN);
            }
        }
    }

    /**
     * Given a revision, read its _attachments dictionary (if any), convert each attachment to a
     * AttachmentInternal object, and return a dictionary mapping names->CBL_Attachments.
     */
    @InterfaceAudience.Private
    public Map<String, AttachmentInternal> getAttachmentsFromRevision(RevisionInternal rev, String prevRevID) throws CouchbaseLiteException {

        Map<String, Object> revAttachments = (Map<String, Object>) rev.getPropertyForKey("_attachments");
        if (revAttachments == null || revAttachments.size() == 0 || rev.isDeleted()) {
            return new HashMap<String, AttachmentInternal>();
        }

        Map<String, AttachmentInternal> attachments = new HashMap<String, AttachmentInternal>();
        for (String name : revAttachments.keySet()) {
            Map<String, Object> attachInfo = (Map<String, Object>) revAttachments.get(name);
            String contentType = (String) attachInfo.get("content_type");
            AttachmentInternal attachment = new AttachmentInternal(name, contentType);
            String newContentBase64 = (String) attachInfo.get("data");
            if (newContentBase64 != null) {
                // If there's inline attachment data, decode and store it:
                byte[] newContents;
                try {
                    newContents = Base64.decode(newContentBase64, Base64.DONT_GUNZIP);
                } catch (IOException e) {
                    throw new CouchbaseLiteException(e, Status.BAD_ENCODING);
                }
                attachment.setLength(newContents.length);
                BlobKey outBlobKey = new BlobKey();
                boolean storedBlob = getAttachments().storeBlob(newContents, outBlobKey);
                attachment.setBlobKey(outBlobKey);
                if (!storedBlob) {
                    throw new CouchbaseLiteException(Status.STATUS_ATTACHMENT_ERROR);
                }
            } else if (attachInfo.containsKey("follows") && ((Boolean) attachInfo.get("follows")).booleanValue() == true) {
                // "follows" means the uploader provided the attachment in a separate MIME part.
                // This means it's already been registered in _pendingAttachmentsByDigest;
                // I just need to look it up by its "digest" property and install it into the store:
                installAttachment(attachment, attachInfo);
            } else {
                // This item is just a stub; validate and skip it
                if (((Boolean) attachInfo.get("stub")).booleanValue() == false) {
                    throw new CouchbaseLiteException("Expected this attachment to be a stub", Status.BAD_ATTACHMENT);
                }
                int revPos = ((Integer) attachInfo.get("revpos")).intValue();
                if (revPos <= 0) {
                    throw new CouchbaseLiteException("Invalid revpos: " + revPos, Status.BAD_ATTACHMENT);
                }
                Map<String, Object> parentAttachments = getAttachments(rev.getDocId(), prevRevID);
                if (parentAttachments != null && parentAttachments.containsKey(name)) {
                    Map<String, Object> parentAttachment = (Map<String, Object>) parentAttachments.get(name);
                    try {
                        BlobKey blobKey = new BlobKey((String) attachInfo.get("digest"));
                        attachment.setBlobKey(blobKey);
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                } else if (parentAttachments == null || !parentAttachments.containsKey(name)) {
                    BlobKey blobKey = null;
                    try {
                        blobKey = new BlobKey((String) attachInfo.get("digest"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                    if (getAttachments().hasBlobForKey(blobKey)) {
                        attachment.setBlobKey(blobKey);
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            // Handle encoded attachment:
            String encodingStr = (String) attachInfo.get("encoding");
            if (encodingStr != null && encodingStr.length() > 0) {
                if (encodingStr.equalsIgnoreCase("gzip")) {
                    attachment.setEncoding(AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP);
                } else {
                    throw new CouchbaseLiteException("Unnkown encoding: " + encodingStr, Status.BAD_ENCODING);
                }
                attachment.setEncodedLength(attachment.getLength());
                if (attachInfo.containsKey("length")) {
                    Number attachmentLength = (Number) attachInfo.get("length");
                    attachment.setLength(attachmentLength.longValue());
                }
            }
            if (attachInfo.containsKey("revpos")) {
                attachment.setRevpos((Integer) attachInfo.get("revpos"));
            }

            attachments.put(name, attachment);
        }

        return attachments;
    }

    /**
     * Given a newly-added revision, adds the necessary attachment rows to the sqliteDb and
     * stores inline attachments into the blob store.
     */
    @InterfaceAudience.Private
    public void processAttachmentsForRevision(Map<String, AttachmentInternal> attachments, RevisionInternal rev, long parentSequence) throws CouchbaseLiteException {

        assert (rev != null);
        long newSequence = rev.getSequence();
        assert (newSequence > parentSequence);
        int generation = rev.getGeneration();
        assert (generation > 0);

        // If there are no attachments in the new rev, there's nothing to do:
        Map<String, Object> revAttachments = null;
        Map<String, Object> properties = (Map<String, Object>) rev.getProperties();
        if (properties != null) {
            revAttachments = (Map<String, Object>) properties.get("_attachments");
        }
        if (revAttachments == null || revAttachments.size() == 0 || rev.isDeleted()) {
            return;
        }

        for (String name : revAttachments.keySet()) {
            AttachmentInternal attachment = attachments.get(name);
            if (attachment != null) {
                // Determine the revpos, i.e. generation # this was added in. Usually this is
                // implicit, but a rev being pulled in replication will have it set already.
                if (attachment.getRevpos() == 0) {
                    attachment.setRevpos(generation);
                } else if (attachment.getRevpos() > generation) {
                    Log.w(Database.TAG, "Attachment %s %s has unexpected revpos %s, setting to %s", rev, name, attachment.getRevpos(), generation);
                    attachment.setRevpos(generation);
                }
                // Finally insert the attachment:
                insertAttachmentForSequence(attachment, newSequence);
            } else {
                // It's just a stub, so copy the previous revision's attachment entry:
                //? Should I enforce that the type and digest (if any) match?
                store.copyAttachmentNamedFromSequenceToSequence(name, parentSequence, newSequence);
            }
        }
    }

    @InterfaceAudience.Private
    public void stubOutAttachmentsInRevision(final Map<String, AttachmentInternal> attachments, final RevisionInternal rev) {

        rev.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                if (attachment.containsKey("follows") || attachment.containsKey("data")) {
                    Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                    editedAttachment.remove("follows");
                    editedAttachment.remove("data");
                    editedAttachment.put("stub", true);
                    if (!editedAttachment.containsKey("revpos")) {
                        editedAttachment.put("revpos", rev.getGeneration());
                    }

                    AttachmentInternal attachmentObject = attachments.get(name);
                    if (attachmentObject != null) {
                        editedAttachment.put("length", attachmentObject.getLength());
                        editedAttachment.put("digest", attachmentObject.getBlobKey().base64Digest());
                    }
                    attachment = editedAttachment;
                }
                return attachment;
            }
        });
    }

    /**
     * in CBLDatabase+Insertion.m
     * - (CBL_Revision*) winnerWithDocID: (SInt64)docNumericID
     * oldWinner: (NSString*)oldWinningRevID
     * oldDeleted: (BOOL)oldWinnerWasDeletion
     * newRev: (CBL_Revision*)newRev
     */
    @InterfaceAudience.Private
    public RevisionInternal winner(long docNumericID,
                                   String oldWinningRevID,
                                   boolean oldWinnerWasDeletion,
                                   RevisionInternal newRev) throws CouchbaseLiteException {

        if (oldWinningRevID == null) {
            return newRev;
        }
        String newRevID = newRev.getRevId();
        if (!newRev.isDeleted()) {
            if (oldWinnerWasDeletion ||
                    RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                return newRev; // this is now the winning live revision
            }
        } else if (oldWinnerWasDeletion) {
            if (RevisionInternal.CBLCompareRevIDs(newRevID, oldWinningRevID) > 0) {
                return newRev;  // doc still deleted, but this beats previous deletion rev
            }
        } else {
            // Doc was alive. How does this deletion affect the winning rev ID?
            AtomicBoolean outIsDeleted = new AtomicBoolean(false);
            AtomicBoolean outIsConflict = new AtomicBoolean(false);
            String winningRevID = winningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
            if (!winningRevID.equals(oldWinningRevID)) {
                if (winningRevID.equals(newRev.getRevId())) {
                    return newRev;
                } else {
                    boolean deleted = false;
                    RevisionInternal winningRev = new RevisionInternal(newRev.getDocId(), winningRevID, deleted);
                    return winningRev;
                }
            }
        }
        return null; // no change
    }

    @InterfaceAudience.Private
    public boolean runFilter(ReplicationFilter filter, Map<String, Object> filterParams, RevisionInternal rev) {
        if (filter == null) {
            return true;
        }
        SavedRevision publicRev = new SavedRevision(this, rev);
        return filter.filter(publicRev, filterParams);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public but Not API
    ///////////////////////////////////////////////////////////////////////////

    @InterfaceAudience.Private
    public interface DatabaseListener {
        void databaseClosing();
    }

    // NOTE: router-only
    @InterfaceAudience.Private
    public void addDatabaseListener(DatabaseListener listener) {
        databaseListeners.addIfAbsent(listener);
    }

    // NOTE: router-only
    @InterfaceAudience.Private
    public void removeDatabaseListener(DatabaseListener listener) {
        databaseListeners.remove(listener);
    }

    /**
     * Get all the active replicators associated with this database.
     */
    @InterfaceAudience.Private
    public List<Replication> getActiveReplications() {
        List<Replication> activeReplicatorsList = new ArrayList<Replication>();
        if (activeReplicators != null) {
            activeReplicatorsList.addAll(activeReplicators);
        }
        return activeReplicatorsList;
    }

    @InterfaceAudience.Private
    public boolean exists() {
        return new File(path).exists();
    }

    @InterfaceAudience.Private
    public synchronized boolean open() {

        if (open) {
            return true;
        }

        // Initialize & open store
        store = new SQLiteStore(path, manager, this);
        //store.setDelegate(this);
        if (!store.open())
            return false;

        // First-time setup:
        if(privateUUID() == null){
            store.setInfo("privateUUID", Misc.CreateUUID());
            store.setInfo("publicUUID", Misc.CreateUUID());
        }

        String sMaxRevs = store.getInfo("max_revs");
        int maxRevs = (sMaxRevs == null) ? DEFAULT_MAX_REVS : Integer.parseInt(sMaxRevs);
        store.setMaxRevTreeDepth(maxRevs);

        // NOTE: Migrate attachment directory path if necessary
        // https://github.com/couchbase/couchbase-lite-java-core/issues/604
        File obsoletedAttachmentStorePath = new File(getObsoletedAttachmentStorePath());
        if (obsoletedAttachmentStorePath != null && obsoletedAttachmentStorePath.exists() && obsoletedAttachmentStorePath.isDirectory()) {
            File attachmentStorePath = new File(getAttachmentStorePath());
            if (attachmentStorePath != null && !attachmentStorePath.exists()) {
                boolean success = obsoletedAttachmentStorePath.renameTo(attachmentStorePath);
                if (!success) {
                    Log.e(Database.TAG, "Could not rename attachment store path");
                    store.close();
                    store = null;
                    return false;
                }
            }
        }

        // NOTE: obsoleted directory is /files/<database name>/attachments/xxxx
        //       Needs to delete /files/<database name>/ too
        File obsoletedAttachmentStoreParentPath = new File(getObsoletedAttachmentStoreParentPath());
        if (obsoletedAttachmentStoreParentPath != null && obsoletedAttachmentStoreParentPath.exists()) {
            obsoletedAttachmentStoreParentPath.delete();
        }

        try {
            if (isBlobstoreMigrated() || !manager.isAutoMigrateBlobStoreFilename()) {
                attachments = new BlobStore(getAttachmentStorePath(), false);
            } else {
                attachments = new BlobStore(getAttachmentStorePath(), true);
                markBlobstoreMigrated();
            }

        } catch (IllegalArgumentException e) {
            Log.e(Database.TAG, "Could not initialize attachment store", e);
            store.close();
            store = null;
            return false;
        }

        open = true;
        return true;
    }

    @InterfaceAudience.Public
    public boolean close() {
        if (!open) {
            return false;
        }

        for (DatabaseListener listener : databaseListeners) {
            listener.databaseClosing();
        }

        if (views != null) {
            for (View view : views.values()) {
                //view.databaseClosing();
                view.close();
            }
        }
        views = null;

        if (activeReplicators != null) {
            List<CountDownLatch> latches = new ArrayList<CountDownLatch>();
            synchronized (activeReplicators) {
                for (Replication replicator : activeReplicators) {
                    // handler to check if the replicator stopped
                    final CountDownLatch latch = new CountDownLatch(1);
                    replicator.addChangeListener(new Replication.ChangeListener() {
                        @Override
                        public void changed(Replication.ChangeEvent event) {
                            if (event.getSource().getStatus() == Replication.ReplicationStatus.REPLICATION_STOPPED) {
                                latch.countDown();
                            }
                        }
                    });
                    latches.add(latch);

                    // ask replicator to stop
                    replicator.databaseClosing();
                }
            }

            // wait till all replicator stopped
            for (CountDownLatch latch : latches) {
                try {
                    boolean success = latch.await(Replication.DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN, TimeUnit.SECONDS);
                    if (!success) {
                        Log.w(Log.TAG_DATABASE, "Replicator could not stop in " + Replication.DEFAULT_MAX_TIMEOUT_FOR_SHUTDOWN + " second.");
                    }
                } catch (Exception e) {
                    Log.w(Log.TAG_DATABASE, e.getMessage());
                }
            }

            activeReplicators = null;
        }

        allReplicators = null;

        if (store != null) store.close();
        store = null;

        open = false;
        return true;
    }

    @InterfaceAudience.Private
    public BlobStoreWriter getAttachmentWriter() {
        return new BlobStoreWriter(getAttachments());
    }

    // NOTE: router-only
    @InterfaceAudience.Private
    public long totalDataSize() {
        File f = new File(path);
        long size = f.length() + attachments.totalDataSize();
        return size;
    }

    @InterfaceAudience.Private
    public boolean beginTransaction() {
        return store == null ? false : store.beginTransaction();
    }

    @InterfaceAudience.Private
    public boolean endTransaction(boolean commit) {
        return store == null ? false : store.endTransaction(commit);
    }

    @InterfaceAudience.Private
    public String privateUUID() {
        return store == null ? null : store.privateUUID();
    }

    @InterfaceAudience.Private
    public String publicUUID() {
        return store == null ? null : store.publicUUID();
    }

    @InterfaceAudience.Private
    public RevisionInternal getDocument(String docID, String revID, EnumSet<TDContentOptions> contentOptions) {
        return store == null ? null : store.getDocument(docID, revID, contentOptions);
    }

    @InterfaceAudience.Private
    public RevisionInternal loadRevisionBody(RevisionInternal rev, EnumSet<TDContentOptions> contentOptions) throws CouchbaseLiteException {
        return store == null ? null : store.loadRevisionBody(rev, contentOptions);
    }

    // Note: unit test only
    @InterfaceAudience.Private
    public long getDocNumericID(String docId) {
        return store == null ? null : store.getDocNumericID(docId);
    }

    /**
     * NOTE: This method is internal use only (from BulkDownloader and PullerInternal)
     */
    @InterfaceAudience.Private
    public List<String> getPossibleAncestorRevisionIDs(RevisionInternal rev, int limit, AtomicBoolean hasAttachment) {
        return store == null ? null : store.getPossibleAncestorRevisionIDs(rev, limit, hasAttachment);
    }

    @InterfaceAudience.Private
    public RevisionList getAllRevisions(String docID, boolean onlyCurrent) {
        return store == null ? null : store.getAllRevisions(docID, onlyCurrent);
    }

    // NOTE: router-only
    @InterfaceAudience.Private
    public String findCommonAncestorOf(RevisionInternal rev, List<String> revIDs) {
        return store == null ? null : store.findCommonAncestor(rev, revIDs);
    }

    /**
     * Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
     */
    @InterfaceAudience.Private
    public Map<String, Object> getRevisionHistoryDictStartingFromAnyAncestor(RevisionInternal rev, List<String> ancestorRevIDs) {
        List<RevisionInternal> history = getRevisionHistory(rev); // (this is in reverse order, newest..oldest
        if (ancestorRevIDs != null && ancestorRevIDs.size() > 0) {
            int n = history.size();
            for (int i = 0; i < n; ++i) {
                if (ancestorRevIDs.contains(history.get(i).getRevId())) {
                    history = history.subList(0, i + 1);
                    break;
                }
            }
        }
        return RevisionUtils.makeRevisionHistoryDict(history);
    }

    @InterfaceAudience.Private
    public RevisionList changesSince(long lastSeq, ChangesOptions options, ReplicationFilter filter, Map<String, Object> filterParams) {
        return store == null ? null : store.changesSince(lastSeq, options, filter, filterParams);
    }

    @InterfaceAudience.Private
    public Map<String, Object> getAllDocs(QueryOptions options) throws CouchbaseLiteException {
        return store == null ? null : store.getAllDocs(options);
    }

    //Note: This method is used only from unit tests.
    @InterfaceAudience.Private
    public void insertAttachmentForSequenceWithNameAndType(InputStream contentStream, long sequence, String name, String contentType, int revpos) throws CouchbaseLiteException {
        assert (sequence > 0);
        assert (name != null);

        BlobKey key = new BlobKey();
        if (!attachments.storeBlobStream(contentStream, key)) {
            throw new CouchbaseLiteException(Status.INTERNAL_SERVER_ERROR);
        }
        insertAttachmentForSequenceWithNameAndType(
                sequence,
                name,
                contentType,
                revpos,
                key);
    }

    /**
     * Returns the content and MIME type of an attachment
     */
    @InterfaceAudience.Private
    public Attachment getAttachmentForSequence(long sequence, String filename) throws CouchbaseLiteException {
        return store == null ? null : store.getAttachmentForSequence(sequence, filename);
    }

    @InterfaceAudience.Private
    public URL fileForAttachmentDict(Map<String, Object> attachmentDict) {
        String digest = (String) attachmentDict.get("digest");
        if (digest == null) {
            return null;
        }
        String path = null;
        Object pending = pendingAttachmentsByDigest.get(digest);
        if (pending != null) {
            if (pending instanceof BlobStoreWriter) {
                path = ((BlobStoreWriter) pending).getFilePath();
            } else {
                BlobKey key = new BlobKey((byte[]) pending);
                path = attachments.pathForKey(key);
            }
        } else {
            // If it's an installed attachment, ask the blob-store for it:
            BlobKey key = new BlobKey(digest);
            path = attachments.pathForKey(key);
        }

        URL retval = null;
        try {
            retval = new File(path).toURI().toURL();
        } catch (MalformedURLException e) {
            //NOOP: retval will be null
        }
        return retval;
    }

    /**
     * Modifies a RevisionInternal's body by changing all attachments with revpos < minRevPos into stubs.
     */
    // NOTE: router-only
    @InterfaceAudience.Private
    public void stubOutAttachmentsIn(RevisionInternal rev, int minRevPos) {
        if (minRevPos <= 1) {
            return;
        }
        Map<String, Object> properties = (Map<String, Object>) rev.getProperties();
        Map<String, Object> attachments = null;
        if (properties != null) {
            attachments = (Map<String, Object>) properties.get("_attachments");
        }
        Map<String, Object> editedProperties = null;
        Map<String, Object> editedAttachments = null;
        for (String name : attachments.keySet()) {
            Map<String, Object> attachment = (Map<String, Object>) attachments.get(name);
            int revPos = (Integer) attachment.get("revpos");
            Object stub = attachment.get("stub");
            if (revPos > 0 && revPos < minRevPos && (stub == null)) {
                // Strip this attachment's body. First make its dictionary mutable:
                if (editedProperties == null) {
                    editedProperties = new HashMap<String, Object>(properties);
                    editedAttachments = new HashMap<String, Object>(attachments);
                    editedProperties.put("_attachments", editedAttachments);
                }
                // ...then remove the 'data' and 'follows' key:
                Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                editedAttachment.remove("data");
                editedAttachment.remove("follows");
                editedAttachment.put("stub", true);
                editedAttachments.put(name, editedAttachment);
                Log.v(Database.TAG, "Stubbed out attachment.  minRevPos: %s rev: %s name: %s revpos: %s", minRevPos, rev, name, revPos);
            }
        }
        if (editedProperties != null)
            rev.setProperties(editedProperties);
    }

    // Replaces attachment data whose revpos is < minRevPos with stubs.
    // If attachmentsFollow==YES, replaces data with "follows" key.
    @InterfaceAudience.Private
    public static void stubOutAttachmentsInRevBeforeRevPos(final RevisionInternal rev, final int minRevPos, final boolean attachmentsFollow) {
        if (minRevPos <= 1 && !attachmentsFollow) {
            return;
        }

        rev.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                int revPos = 0;
                if (attachment.get("revpos") != null) {
                    revPos = (Integer) attachment.get("revpos");
                }

                boolean includeAttachment = (revPos == 0 || revPos >= minRevPos);
                boolean stubItOut = !includeAttachment && (attachment.get("stub") == null || (Boolean) attachment.get("stub") == false);
                boolean addFollows = includeAttachment && attachmentsFollow && (attachment.get("follows") == null || (Boolean) attachment.get("follows") == false);

                if (!stubItOut && !addFollows) {
                    return attachment;  // no change
                }

                // Need to modify attachment entry:
                Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                editedAttachment.remove("data");
                if (stubItOut) {
                    // ...then remove the 'data' and 'follows' key:
                    editedAttachment.remove("follows");
                    editedAttachment.put("stub", true);
                    Log.v(Log.TAG_SYNC, "Stubbed out attachment %s: revpos %d < %d", rev, revPos, minRevPos);
                } else if (addFollows) {
                    editedAttachment.remove("stub");
                    editedAttachment.put("follows", true);
                    Log.v(Log.TAG_SYNC, "Added 'follows' for attachment %s: revpos %d >= %d", rev, revPos, minRevPos);
                }
                return editedAttachment;
            }
        });
    }

    // Replaces the "follows" key with the real attachment data in all attachments to 'doc'.
    @InterfaceAudience.Private
    public boolean inlineFollowingAttachmentsIn(RevisionInternal rev) {

        return rev.mutateAttachments(new CollectionUtils.Functor<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> invoke(Map<String, Object> attachment) {
                if (!attachment.containsKey("follows")) {
                    return attachment;
                }
                URL fileURL = fileForAttachmentDict(attachment);
                byte[] fileData = null;
                try {
                    InputStream is = fileURL.openStream();
                    ByteArrayOutputStream os = new ByteArrayOutputStream();
                    StreamUtils.copyStream(is, os);
                    fileData = os.toByteArray();
                } catch (IOException e) {
                    Log.e(Log.TAG_SYNC, "could not retrieve attachment data: %S", e);
                    return null;
                }

                Map<String, Object> editedAttachment = new HashMap<String, Object>(attachment);
                editedAttachment.remove("follows");
                editedAttachment.put("data", Base64.encodeBytes(fileData));
                return editedAttachment;
            }
        });
    }

    /**
     * Updates or deletes an attachment, creating a new document revision in the process.
     * Used by the PUT / DELETE methods called on attachment URLs.
     */
    @InterfaceAudience.Private
    public RevisionInternal updateAttachment(String filename, BlobStoreWriter body, String contentType, AttachmentInternal.AttachmentEncoding encoding, String docID, String oldRevID) throws CouchbaseLiteException {

        boolean isSuccessful = false;

        if (filename == null || filename.length() == 0 || (body != null && contentType == null) || (oldRevID != null && docID == null) || (body != null && docID == null)) {
            throw new CouchbaseLiteException(Status.BAD_REQUEST);
        }

        beginTransaction();
        try {
            RevisionInternal oldRev = new RevisionInternal(docID, oldRevID, false);
            if (oldRevID != null) {

                // Load existing revision if this is a replacement:
                try {
                    loadRevisionBody(oldRev, EnumSet.noneOf(TDContentOptions.class));
                } catch (CouchbaseLiteException e) {
                    if (e.getCBLStatus().getCode() == Status.NOT_FOUND && store.existsDocumentWithIDAndRev(docID, null)) {
                        throw new CouchbaseLiteException(Status.CONFLICT);
                    }
                }

            } else {
                // If this creates a new doc, it needs a body:
                oldRev.setBody(new Body(new HashMap<String, Object>()));
            }

            // Update the _attachments dictionary:
            Map<String, Object> oldRevProps = oldRev.getProperties();
            Map<String, Object> attachments = null;
            if (oldRevProps != null) {
                attachments = (Map<String, Object>) oldRevProps.get("_attachments");
            }

            if (attachments == null)
                attachments = new HashMap<String, Object>();

            if (body != null) {
                BlobKey key = body.getBlobKey();
                String digest = key.base64Digest();

                Map<String, BlobStoreWriter> blobsByDigest = new HashMap<String, BlobStoreWriter>();
                blobsByDigest.put(digest, body);
                rememberAttachmentWritersForDigests(blobsByDigest);

                String encodingName = (encoding == AttachmentInternal.AttachmentEncoding.AttachmentEncodingGZIP) ? "gzip" : null;
                Map<String, Object> dict = new HashMap<String, Object>();

                dict.put("digest", digest);
                dict.put("length", body.getLength());
                dict.put("follows", true);
                dict.put("content_type", contentType);
                dict.put("encoding", encodingName);

                attachments.put(filename, dict);
            } else {
                if (oldRevID != null && !attachments.containsKey(filename)) {
                    throw new CouchbaseLiteException(Status.NOT_FOUND);
                }
                attachments.remove(filename);
            }

            Map<String, Object> properties = oldRev.getProperties();
            properties.put("_attachments", attachments);
            oldRev.setProperties(properties);


            // Create a new revision:
            Status putStatus = new Status();
            RevisionInternal newRev = putRevision(oldRev, oldRevID, false, putStatus);

            isSuccessful = true;
            return newRev;

        } catch (SQLException e) {
            Log.e(TAG, "Error updating attachment", e);
            throw new CouchbaseLiteException(new Status(Status.INTERNAL_SERVER_ERROR));
        } finally {
            endTransaction(isSuccessful);
        }
    }

    @InterfaceAudience.Private
    public void rememberAttachmentWritersForDigests(Map<String, BlobStoreWriter> blobsByDigest) {
        getPendingAttachmentsByDigest().putAll(blobsByDigest);
    }

    @InterfaceAudience.Private
    public static String generateDocumentId() {
        return Misc.CreateUUID();
    }

    /**
     * Parses the _revisions dict from a document into an array of revision ID strings
     */
    @InterfaceAudience.Private
    public static List<String> parseCouchDBRevisionHistory(Map<String, Object> docProperties) {
        Map<String, Object> revisions = (Map<String, Object>) docProperties.get("_revisions");
        if (revisions == null) {
            return new ArrayList<String>();
        }
        List<String> revIDs = new ArrayList<String>((List<String>) revisions.get("ids"));
        if (revIDs == null || revIDs.isEmpty()) {
            return new ArrayList<String>();
        }
        Integer start = (Integer) revisions.get("start");
        if (start != null) {
            for (int i = 0; i < revIDs.size(); i++) {
                String revID = revIDs.get(i);
                revIDs.set(i, Integer.toString(start--) + "-" + revID);
            }
        }
        return revIDs;
    }

    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal rev, String prevRevId, boolean allowConflict) throws CouchbaseLiteException {
        Status ignoredStatus = new Status();
        return putRevision(rev, prevRevId, allowConflict, ignoredStatus);
    }

    /**
     * Stores a new (or initial) revision of a document.
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
    @InterfaceAudience.Private
    public RevisionInternal putRevision(RevisionInternal oldRev, String prevRevId, boolean allowConflict, Status resultStatus) throws CouchbaseLiteException {
        return store == null ? null : store.putRevision(oldRev, prevRevId, allowConflict, resultStatus);
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
    @InterfaceAudience.Private
    public void forceInsert(RevisionInternal rev, List<String> revHistory, URL source) throws CouchbaseLiteException {
        if (store != null)
            store.forceInsert(rev, revHistory, source);
    }

    @InterfaceAudience.Private
    public String lastSequenceWithCheckpointId(String checkpointId) {
        return store == null ? null : store.getInfo(checkpointInfoKey(checkpointId));
    }

    @InterfaceAudience.Private
    public boolean setLastSequence(String lastSequence, String checkpointId) {
        return store == null ? false : store.setInfo(checkpointInfoKey(checkpointId), lastSequence) != -1;
    }

    private static String checkpointInfoKey(String checkpointID) {
        return "checkpoint/" + checkpointID;
    }

    @InterfaceAudience.Private
    public int findMissingRevisions(RevisionList touchRevs) throws SQLException {
        return store == null ? 0 : store.findMissingRevisions(touchRevs);
    }

    @InterfaceAudience.Private
    public RevisionInternal putLocalRevision(RevisionInternal revision, String prevRevID) throws CouchbaseLiteException {
        return store == null ? null : store.putLocalRevision(revision, prevRevID);
    }

    /**
     * Purges specific revisions, which deletes them completely from the local database _without_ adding a "tombstone" revision. It's as though they were never there.
     * This operation is described here: http://wiki.apache.org/couchdb/Purge_Documents
     *
     * @param docsToRevs A dictionary mapping document IDs to arrays of revision IDs.
     * @resultOn success will point to an NSDictionary with the same form as docsToRev, containing the doc/revision IDs that were actually removed.
     */
    @InterfaceAudience.Private
    public Map<String, Object> purgeRevisions(final Map<String, List<String>> docsToRevs) {
        return store == null ? null : store.purgeRevisions(docsToRevs);
    }

    @InterfaceAudience.Private
    public RevisionInternal getLocalDocument(String docID, String revID) {
        return store == null ? null : store.getLocalDocument(docID, revID);
    }

    // NOTE: router-only
    @InterfaceAudience.Private
    public long getStartTime() {
        return this.startTime;
    }

    /**
     * Is the database open?
     */
    @InterfaceAudience.Private
    public boolean isOpen() {
        return open;
    }

    @InterfaceAudience.Private
    public void addReplication(Replication replication) {
        if (allReplicators != null) {
            allReplicators.add(replication);
        }
    }

    @InterfaceAudience.Private
    public void addActiveReplication(Replication replication) {
        replication.addChangeListener(new Replication.ChangeListener() {
            @Override
            public void changed(Replication.ChangeEvent event) {
                if (event.getTransition() != null && event.getTransition().getDestination() == ReplicationState.STOPPED) {
                    if (activeReplicators != null) {
                        activeReplicators.remove(event.getSource());
                    }
                }
            }
        });

        if (activeReplicators != null) {
            activeReplicators.add(replication);
        }
    }

    /**
     * Get the PersistentCookieStore associated with this database.
     * Will lazily create one if none exists.
     */
    @InterfaceAudience.Private
    public PersistentCookieStore getPersistentCookieStore() {
        if (persistentCookieStore == null)
            persistentCookieStore = new PersistentCookieStore(this);
        return persistentCookieStore;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (protected or private) Methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Set the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    protected void setMaxRevTreeDepth(int maxRevTreeDepth) {
        if (store != null)
            store.setMaxRevTreeDepth(maxRevTreeDepth);
    }

    /**
     * Get the maximum depth of a document's revision tree (or, max length of its revision history.)
     * Revisions older than this limit will be deleted during a -compact: operation.
     * Smaller values save space, at the expense of making document conflicts somewhat more likely.
     */
    protected int getMaxRevTreeDepth() {
        return store == null ? 0 : store.getMaxRevTreeDepth();
    }

    /**
     * Empties the cache of recently used Document objects.
     * API calls will now instantiate and return new instances.
     */
    protected void clearDocumentCache() {
        docCache.clear();
    }

    /**
     * Returns the already-instantiated cached Document with the given ID, or nil if none is yet cached.
     */
    protected Document getCachedDocument(String documentID) {
        return docCache.get(documentID);
    }

    protected void removeDocumentFromCache(Document document) {
        docCache.remove(document.getId());
    }

    protected String getAttachmentStorePath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if (lastDotPosition > 0) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        attachmentStorePath = attachmentStorePath + " attachments";
        return attachmentStorePath;
    }

    private String getObsoletedAttachmentStorePath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if (lastDotPosition > 0) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        attachmentStorePath = attachmentStorePath + File.separator + "attachments";
        return attachmentStorePath;
    }

    private String getObsoletedAttachmentStoreParentPath() {
        String attachmentStorePath = path;
        int lastDotPosition = attachmentStorePath.lastIndexOf('.');
        if (lastDotPosition > 0) {
            attachmentStorePath = attachmentStorePath.substring(0, lastDotPosition);
        }
        return attachmentStorePath;
    }

    private boolean isBlobstoreMigrated() {
        Map<String, Object> props = getExistingLocalDocument("_blobstore");
        if (props != null && props.containsKey("blobstoreMigrated"))
            return (Boolean) props.get("blobstoreMigrated");
        return false;
    }

    private void markBlobstoreMigrated() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("blobstoreMigrated", true);
        try {
            putLocalDocument("_blobstore", props);
        } catch (CouchbaseLiteException e) {
            Log.e(Log.TAG_DATABASE, e.getMessage());
        }
    }

    protected String getPath() {
        return path;
    }

    protected SQLiteStorageEngine getDatabase() {
        return store.getStorageEngine();
    }

    public Store getStore() {
        return store;
    }

    /**
     * Returns an array of TDRevs in reverse chronological order, starting with the given revision.
     */
    protected List<RevisionInternal> getRevisionHistory(RevisionInternal rev) {
        return store == null ? null : store.getRevisionHistory(rev);
    }

    private String getDesignDocFunction(String fnName, String key, List<String> outLanguageList) {
        String[] path = fnName.split("/");
        if (path.length != 2) {
            return null;
        }
        String docId = String.format("_design/%s", path[0]);
        RevisionInternal rev = getDocument(docId, null, EnumSet.noneOf(TDContentOptions.class));
        if (rev == null) {
            return null;
        }

        String outLanguage = (String) rev.getPropertyForKey("language");
        if (outLanguage != null) {
            outLanguageList.add(outLanguage);
        } else {
            outLanguageList.add("javascript");
        }
        Map<String, Object> container = (Map<String, Object>) rev.getPropertyForKey(key);
        return (String) container.get(path[1]);
    }

    /**
     * in CBLDatabase+Internal.m
     * - (void) forgetViewNamed: (NSString*)name
     */
    protected void forgetView(String name){
        views.remove(name);
    }

    private View registerView(View view) {
        if (view == null) {
            return null;
        }
        if (views == null) {
            views = new HashMap<String, View>();
        }
        views.put(view.getName(), view);
        return view;
    }

    protected List<QueryRow> queryViewNamed(String viewName, QueryOptions options, List<Long> outLastSequence) throws CouchbaseLiteException {

        long before = System.currentTimeMillis();
        long lastSequence = 0;
        List<QueryRow> rows = null;

        if (viewName != null && viewName.length() > 0) {
            final View view = getView(viewName);
            if (view == null) {
                throw new CouchbaseLiteException(new Status(Status.NOT_FOUND));
            }
            lastSequence = view.getLastSequenceIndexed();
            if (options.getStale() == Query.IndexUpdateMode.BEFORE || lastSequence <= 0) {
                view.updateIndex();
                lastSequence = view.getLastSequenceIndexed();
            } else if (options.getStale() == Query.IndexUpdateMode.AFTER && lastSequence < getLastSequenceNumber()) {

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            view.updateIndex();
                        } catch (CouchbaseLiteException e) {
                            Log.e(Database.TAG, "Error updating view index on background thread", e);
                        }
                    }
                }).start();
            }
            rows = view.query(options);
        } else {
            // nil view means query _all_docs
            // note: this is a little kludgy, but we have to pull out the "rows" field from the
            // result dictionary because that's what we want.  should be refactored, but
            // it's a little tricky, so postponing.
            Map<String, Object> allDocsResult = getAllDocs(options);
            rows = (List<QueryRow>) allDocsResult.get("rows");
            lastSequence = getLastSequenceNumber();
        }
        outLastSequence.add(lastSequence);

        long delta = System.currentTimeMillis() - before;
        Log.d(Database.TAG, "Query view %s completed in %d milliseconds", viewName, delta);

        return rows;
    }

    protected View makeAnonymousView() {
        for (int i = 0; true; ++i) {
            String name = String.format("anon%d", i);
            View existing = getExistingView(name);
            if (existing == null) {
                // this name has not been used yet, so let's use it
                return getView(name);
            }
        }
    }

    /**
     * NOTE: Only used by Unit Tests
     */
    protected List<View> getAllViews() {
        //return store == null ? null : store.getAllViews();
        List<String> names = store == null ? null : store.getAllViewNames();
        if(names == null)
            return null;
        List<View> views = new ArrayList<View>();
        for(String name : names){
            views.add(this.getExistingView(name));
        }
        return views;
    }
/*
    protected Status deleteViewNamed(String name) {
        return store == null ? null : store.deleteViewNamed(name);
    }
*/
    /**
     * Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
     * <p/>
     * in CBLDatabase+Internal.m
     * - (NSString*) winningRevIDOfDocNumericID: (SInt64)docNumericID
     * isDeleted: (BOOL*)outIsDeleted
     * isConflict: (BOOL*)outIsConflict // optional
     * status: (CBLStatus*)outStatus
     * NOTE: Called only from internal and Unit Tests
     */
    protected String winningRevIDOfDoc(long docNumericId, AtomicBoolean outIsDeleted, AtomicBoolean outIsConflict) throws CouchbaseLiteException {
        return store == null ? null : store.winningRevIDOfDoc(docNumericId, outIsDeleted, outIsConflict);
    }

    // Database+Attachments

    private void insertAttachmentForSequence(AttachmentInternal attachment, long sequence) throws CouchbaseLiteException {
        if (store != null)
            store.insertAttachmentForSequenceWithNameAndType(
                    sequence,
                    attachment.getName(),
                    attachment.getContentType(),
                    attachment.getRevpos(),
                    attachment.getBlobKey(),
                    attachment.getLength(),
                    attachment.getEncoding(),
                    attachment.getEncodedLength());
    }

    private void insertAttachmentForSequenceWithNameAndType(long sequence, String name, String contentType, int revpos, BlobKey key) throws CouchbaseLiteException {
        if (store != null)
            store.insertAttachmentForSequenceWithNameAndType(sequence, name, contentType, revpos, key, key != null ? attachments.getSizeOfBlob(key) : -1, AttachmentInternal.AttachmentEncoding.AttachmentEncodingNone, -1);
    }

    private void installAttachment(AttachmentInternal attachment, Map<String, Object> attachInfo) throws CouchbaseLiteException {
        String digest = (String) attachInfo.get("digest");
        if (digest == null) {
            throw new CouchbaseLiteException(Status.BAD_ATTACHMENT);
        }

        if (pendingAttachmentsByDigest != null && pendingAttachmentsByDigest.containsKey(digest)) {
            BlobStoreWriter writer = pendingAttachmentsByDigest.get(digest);
            try {
                BlobStoreWriter blobStoreWriter = (BlobStoreWriter) writer;
                blobStoreWriter.install();
                attachment.setBlobKey(blobStoreWriter.getBlobKey());
                attachment.setLength(blobStoreWriter.getLength());
            } catch (Exception e) {
                throw new CouchbaseLiteException(e, Status.STATUS_ATTACHMENT_ERROR);
            }
        } else {
            Log.w(Database.TAG, "No pending attachment for digest: " + digest);
            throw new CouchbaseLiteException(Status.BAD_ATTACHMENT);
        }
    }

    private Map<String, BlobStoreWriter> getPendingAttachmentsByDigest() {
        if (pendingAttachmentsByDigest == null)
            pendingAttachmentsByDigest = new HashMap<String, BlobStoreWriter>();
        return pendingAttachmentsByDigest;
    }

    protected void copyAttachmentNamedFromSequenceToSequence(String name, long fromSeq, long toSeq) throws CouchbaseLiteException {
        if (store != null)
            store.copyAttachmentNamedFromSequenceToSequence(name, fromSeq, toSeq);
    }

    /**
     * Returns the location of an attachment's file in the blob store.
     * NOTE: Used by only from Attachment
     */
    protected String getAttachmentPathForSequence(long sequence, String filename) throws CouchbaseLiteException {
        return store == null ? null : store.getAttachmentPathForSequence(sequence, filename);
    }

    /**
     * Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
     * TODO: do we need this in Database.java???
     */
    protected Map<String, Object> getAttachmentsDictForSequenceWithContent(long sequence, EnumSet<TDContentOptions> contentOptions) {
        return store == null ? null : store.getAttachmentsDictForSequenceWithContent(sequence, contentOptions);
    }

    protected void rememberAttachmentWriter(BlobStoreWriter writer) {
        getPendingAttachmentsByDigest().put(writer.mD5DigestString(), writer);
    }

    // Database+Insertion

    private boolean postChangeNotifications() {
        boolean posted = false;
        // This is a 'while' instead of an 'if' because when we finish posting notifications, there
        // might be new ones that have arrived as a result of notification handlers making document
        // changes of their own (the replicator manager will do this.) So we need to check again.
        while (!store.inTransaction() && isOpen() && !postingChangeNotifications
                && changesToNotify.size() > 0) {

            try {
                postingChangeNotifications = true; // Disallow re-entrant calls

                List<DocumentChange> outgoingChanges = new ArrayList<DocumentChange>();
                outgoingChanges.addAll(changesToNotify);
                changesToNotify.clear();

                // TODO: postPublicChangeNotification in CBLDatabase+Internal.m should replace following lines of code.

                boolean isExternal = false;
                for (DocumentChange change : outgoingChanges) {
                    Document doc = cachedDocumentWithID(change.getDocumentId());
                    if (doc != null) {
                        doc.revisionAdded(change, true);
                    }
                    if (change.getSourceUrl() != null) {
                        isExternal = true;
                    }
                }

                ChangeEvent changeEvent = new ChangeEvent(this, isExternal, outgoingChanges);

                for (ChangeListener changeListener : changeListeners) {
                    changeListener.changed(changeEvent);
                }

                posted = true;
            } catch (Exception e) {
                Log.e(Database.TAG, this + " got exception posting change notifications", e);
            } finally {
                postingChangeNotifications = false;
            }
        }
        return posted;
    }

    protected Map<String, Object> getAttachments(String docID, String revID) {
        RevisionInternal mrev = new RevisionInternal(docID, revID, false);
        try {
            RevisionInternal rev = loadRevisionBody(mrev, EnumSet.noneOf(TDContentOptions.class));
            return rev.getAttachments();
        } catch (CouchbaseLiteException e) {
            Log.w(Log.TAG_DATABASE, "Failed to get attachments for " + mrev, e);
            return null;
        }
    }

    // Database+Replication

    protected Replication getActiveReplicator(URL remote, boolean push) {
        if (activeReplicators != null) {
            synchronized (activeReplicators) {
                for (Replication replicator : activeReplicators) {
                    if (replicator.getRemoteUrl().equals(remote) && replicator.isPull() == !push && replicator.isRunning()) {
                        return replicator;
                    }
                }
            }
        }
        return null;
    }

    protected Replication getReplicator(URL remote, HttpClientFactory httpClientFactory, boolean push, boolean continuous, ScheduledExecutorService workExecutor) {
        Replication result = getActiveReplicator(remote, push);
        if (result != null) {
            return result;
        }
        if (push) {
            result = new Replication(this, remote, Replication.Direction.PUSH, httpClientFactory, workExecutor);
        } else {
            result = new Replication(this, remote, Replication.Direction.PULL, httpClientFactory, workExecutor);
        }
        result.setContinuous(continuous);
        return result;
    }

    // Database+LocalDocs                                                                      ***/

    private static String makeLocalDocumentId(String documentId) {
        return String.format("_local/%s", documentId);
    }

    /**
     * Creates a one-shot query with the given map function. This is equivalent to creating an
     * anonymous View and then deleting it immediately after querying it. It may be useful during
     * development, but in general this is inefficient if this map will be used more than once,
     * because the entire view has to be regenerated from scratch every time.
     */
    protected Query slowQuery(Mapper map) {
        return new Query(this, map);
    }

    protected RevisionInternal getParentRevision(RevisionInternal rev) {
        return store == null ? null : store.getParentRevision(rev);
    }

    protected boolean replaceUUIDs() {
        return store == null ? false : store.replaceUUIDs();
    }

    /**
     * Set the database's name.
     */
    protected void setName(String name) {
        this.name = name;
    }

    private Document cachedDocumentWithID(String documentId) {
        return docCache.resourceWithCacheKeyDontRecache(documentId);
    }
}
