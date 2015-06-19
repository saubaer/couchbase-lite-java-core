package com.couchbase.lite;

public interface QueryRowStorage {
    /**
     * Given the raw data of a row's value, returns YES if this is a non-JSON placeholder representing
     * the entire document. If so, the CBLQueryRow will not parse this data but will instead fetch the
     * document's body from the database and use that as its value.
     */
    boolean rowValueIsEntireDoc(byte[] valueData);

    /**
     * Parses a "normal" (not entire-doc) row value into a JSON-compatible object.
     */
    Object parseRowValue(byte[] valueData);
}
