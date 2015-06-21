package com.couchbase.lite.util;

import com.couchbase.lite.storage.Cursor;
import com.couchbase.lite.storage.SQLException;
import com.couchbase.lite.storage.SQLiteStorageEngine;

public class SQLiteUtils {
    public static byte[] byteArrayResultForQuery(SQLiteStorageEngine storageEngine, String query, String[] args) throws SQLException {
        byte[] result = null;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getBlob(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }

    public static long longForQuery(SQLiteStorageEngine storageEngine, String query, String[] args) throws SQLException {
        Cursor cursor = null;
        long result = -1;
        try {
            cursor = storageEngine.rawQuery(query, args);
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
    public static int intForQuery(SQLiteStorageEngine storageEngine, String query, String[] args) throws SQLException {
        Cursor cursor = null;
        int result = -1;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getInt(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }
    public static boolean booleanForQuery(SQLiteStorageEngine storageEngine, String query, String[] args) throws SQLException {
        boolean result = false;
        Cursor cursor = null;
        try {
            cursor = storageEngine.rawQuery(query, args);
            if (cursor.moveToNext()) {
                result = cursor.getLong(0) == 1L;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return result;
    }
}
