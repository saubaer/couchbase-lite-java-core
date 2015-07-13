package com.couchbase.lite.internal;

import com.couchbase.lite.BlobKey;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple container for attachment metadata.
 */
public class AttachmentInternal {

    public enum AttachmentEncoding {
        AttachmentEncodingNone, AttachmentEncodingGZIP
    }

    private String name;
    private String contentType;

    private BlobKey blobKey;
    private long length;
    private long encodedLength;
    private AttachmentEncoding encoding;
    private int revpos;

    public AttachmentInternal(String name, String contentType) {
        this.name = name;
        this.contentType = contentType;
    }

    public AttachmentInternal(String name, Map<String, Object> attachInfo) {
        this(name, (String)attachInfo.get("content_type"));

        // TODO
    }

    public boolean isValid() {
        if (encoding != AttachmentEncoding.AttachmentEncodingNone) {
            if (encodedLength == 0 && length > 0) {
                return false;
            }
        }
        else if (encodedLength > 0) {
            return false;
        }
        if (revpos == 0) {
            return false;
        }
        return true;
    }

    public Map<String, Object> asStubDictionary(){
        Map<String, Object> dict = new HashMap<String, Object>();
        dict.put("stub", true);
        dict.put("digest", blobKey.base64Digest());
        dict.put("content_type", contentType);
        dict.put("revpos", revpos);
        dict.put("length", length);
        if(encodedLength > 0)
            dict.put("encoded_length", encodedLength);
        switch (encoding){
            case AttachmentEncodingGZIP:
                dict.put("encoding", "gzip");
                break;
            case AttachmentEncodingNone:
                break;
        }
        return dict;
    }

    public String getName() {
        return name;
    }

    public String getContentType() {
        return contentType;
    }

    public BlobKey getBlobKey() {
        return blobKey;
    }

    public void setBlobKey(BlobKey blobKey) {
        this.blobKey = blobKey;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getEncodedLength() {
        return encodedLength;
    }

    public void setEncodedLength(long encodedLength) {
        this.encodedLength = encodedLength;
    }

    public AttachmentEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(AttachmentEncoding encoding) {
        this.encoding = encoding;
    }

    public int getRevpos() {
        return revpos;
    }

    public void setRevpos(int revpos) {
        this.revpos = revpos;
    }


}

