package com.couchbase.lite.support;

import com.couchbase.lite.Database;
import com.couchbase.lite.Manager;
import com.couchbase.lite.internal.AttachmentInternal;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.util.Log;
import com.couchbase.lite.util.Utils;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by hideki on 6/17/15.
 */
public class RevisionUtils {

    private static final Set<String> KNOWN_SPECIAL_KEYS;

    static {
        KNOWN_SPECIAL_KEYS = new HashSet<String>();
        KNOWN_SPECIAL_KEYS.add("_id");
        KNOWN_SPECIAL_KEYS.add("_rev");
        KNOWN_SPECIAL_KEYS.add("_attachments");
        KNOWN_SPECIAL_KEYS.add("_deleted");
        KNOWN_SPECIAL_KEYS.add("_revisions");
        KNOWN_SPECIAL_KEYS.add("_revs_info");
        KNOWN_SPECIAL_KEYS.add("_conflicts");
        KNOWN_SPECIAL_KEYS.add("_deleted_conflicts");
        KNOWN_SPECIAL_KEYS.add("_local_seq");
        KNOWN_SPECIAL_KEYS.add("_removed");
    }

    public static Map<String, Object> makeRevisionHistoryDict(List<RevisionInternal> history) {
        if (history == null) {
            return null;
        }

        // Try to extract descending numeric prefixes:
        List<String> suffixes = new ArrayList<String>();
        int start = -1;
        int lastRevNo = -1;
        for (RevisionInternal rev : history) {
            int revNo = parseRevIDNumber(rev.getRevId());
            String suffix = parseRevIDSuffix(rev.getRevId());
            if (revNo > 0 && suffix.length() > 0) {
                if (start < 0) {
                    start = revNo;
                } else if (revNo != lastRevNo - 1) {
                    start = -1;
                    break;
                }
                lastRevNo = revNo;
                suffixes.add(suffix);
            } else {
                start = -1;
                break;
            }
        }

        Map<String, Object> result = new HashMap<String, Object>();
        if (start == -1) {
            // we failed to build sequence, just stuff all the revs in list
            suffixes = new ArrayList<String>();
            for (RevisionInternal rev : history) {
                suffixes.add(rev.getRevId());
            }
        } else {
            result.put("start", start);
        }
        result.put("ids", suffixes);

        return result;
    }

    /**
     * Splits a revision ID into its generation number and opaque suffix string
     */
    public static int parseRevIDNumber(String rev) {
        int result = -1;
        int dashPos = rev.indexOf("-");
        if (dashPos >= 0) {
            try {
                result = Integer.parseInt(rev.substring(0, dashPos));
            } catch (NumberFormatException e) {
                // ignore, let it return -1
            }
        }
        return result;
    }

    /**
     * Splits a revision ID into its generation number and opaque suffix string
     */
    public static String parseRevIDSuffix(String rev) {
        String result = null;
        int dashPos = rev.indexOf("-");
        if (dashPos >= 0) {
            result = rev.substring(dashPos + 1);
        }
        return result;
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    public static byte[] encodeDocumentJSON(RevisionInternal rev) {

        Map<String, Object> origProps = rev.getProperties();
        if (origProps == null) {
            return null;
        }

        List<String> specialKeysToLeave = Arrays.asList(
                "_removed",
                "_replication_id",
                "_replication_state",
                "_replication_state_time");

        // Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
        Map<String, Object> properties = new HashMap<String, Object>(origProps.size());
        for (String key : origProps.keySet()) {
            boolean shouldAdd = false;
            if (key.startsWith("_")) {
                if (!KNOWN_SPECIAL_KEYS.contains(key)) {
                    Log.e(Database.TAG, "Database: Invalid top-level key '%s' in document to be inserted", key);
                    return null;
                }
                if (specialKeysToLeave.contains(key)) {
                    shouldAdd = true;
                }
            } else {
                shouldAdd = true;
            }
            if (shouldAdd) {
                properties.put(key, origProps.get(key));
            }
        }

        byte[] json = null;
        try {
            json = Manager.getObjectMapper().writeValueAsBytes(properties);
        } catch (Exception e) {
            Log.e(Database.TAG, "Error serializing " + rev + " to JSON", e);
        }
        return json;
    }


    /**
     * in CBLDatabase+Insertion.m
     * - (NSString*) generateIDForRevision: (CBL_Revision*)rev
     *                            withJSON: (NSData*)json
     *                         attachments: (NSDictionary*)attachments
     *                              prevID: (NSString*) prevID
     * @exclude
     */
    @InterfaceAudience.Private
    public static String generateIDForRevision(RevisionInternal rev, byte[] json, Map<String, AttachmentInternal> attachments, String previousRevisionId) {

        MessageDigest md5Digest;

        // Revision IDs have a generation count, a hyphen, and a UUID.

        int generation = 0;
        if(previousRevisionId != null) {
            generation = RevisionInternal.generationFromRevID(previousRevisionId);
            if(generation == 0) {
                return null;
            }
        }

        // Generate a digest for this revision based on the previous revision ID, document JSON,
        // and attachment digests. This doesn't need to be secure; we just need to ensure that this
        // code consistently generates the same ID given equivalent revisions.
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        // single byte - length of previous revision id
        // +
        // previous revision id
        int length = 0;
        byte[] prevIDUTF8 = null;
        if (previousRevisionId != null) {
            prevIDUTF8 = previousRevisionId.getBytes(Charset.forName("UTF-8"));
            length = prevIDUTF8.length;
        }
        if (length > 0xFF) {
            return null;
        }
        byte lengthByte = (byte) (length & 0xFF);
        byte[] lengthBytes = new byte[] { lengthByte };
        md5Digest.update(lengthBytes); // prefix with length byte
        if (length > 0 && prevIDUTF8 != null) {
            md5Digest.update(prevIDUTF8);
        }

        // single byte - deletion flag
        int isDeleted = ((rev.isDeleted() != false) ? 1 : 0);
        byte[] deletedByte = new byte[] { (byte) isDeleted };
        md5Digest.update(deletedByte);

        // all attachment keys
        List<String> attachmentKeys = new ArrayList<String>(attachments.keySet());
        Collections.sort(attachmentKeys);
        for (String key : attachmentKeys) {
            AttachmentInternal attachment = attachments.get(key);
            md5Digest.update(attachment.getBlobKey().getBytes());
        }

        // json
        if (json != null) {
            md5Digest.update(json);
        }
        byte[] md5DigestResult = md5Digest.digest();

        String digestAsHex = Utils.bytesToHex(md5DigestResult);

        int generationIncremented = generation + 1;
        return String.format("%d-%s", generationIncremented, digestAsHex).toLowerCase();
    }
}

