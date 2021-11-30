package com.pantheon.server.registry;

/**
 * @author Anthony
 * @create 2021/11/30
 * @desc
 */
public class Key {
    private final String entityName;
    private final String hashKey;
    private final ACCEPT accept;

    public enum ACCEPT {
        FULL, COMPACT
    }


    public Key(String entityName,ACCEPT accept) {
        this.entityName = entityName;
        this.accept =accept;
        hashKey = this.entityName+this.accept;
    }

    public String getName() {
        return entityName;
    }

    public String getHashKey() {
        return hashKey;
    }


    @Override
    public int hashCode() {
        String hashKey = getHashKey();
        return hashKey.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Key) {
            return getHashKey().equals(((Key) other).getHashKey());
        } else {
            return false;
        }
    }


    public String toStringCompact() {
        StringBuilder sb = new StringBuilder();
        sb.append("{name=").append(entityName).append(", accept=").append(accept).append('}');
        return sb.toString();
    }
}
