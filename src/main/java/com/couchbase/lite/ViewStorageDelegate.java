package com.couchbase.lite;

/**
 * Delegate of a CBL_ViewStorage instance. CBLView implements this.
 */
public interface ViewStorageDelegate {
    /**
     * The current map block. Never nil.
     */
    Mapper getMap();

    /**
     * The current reduce block, or nil if there is none.
     */
    Reducer getReduce();

    /**
     * The current map version string. If this changes, the storage's -setVersion: method will be
     * called to notify it, so it can invalidate the index.
     */
    String getMapVersion();

    /**
     * The document "type" property values this view is filtered to (nil if none.)
     */
    String getDocumentType();
}
