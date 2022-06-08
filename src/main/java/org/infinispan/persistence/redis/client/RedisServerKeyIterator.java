package org.infinispan.persistence.redis.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Iterator;
import java.util.List;

final public class RedisServerKeyIterator implements Iterator<byte[]> {
    private static final int SCAN_COUNT_SIZE = 100;

    private final Jedis client;
    private ScanResult<byte[]> scanCursor;
    private List<byte[]> keyResults;
    private int position = 0;
    private final ScanParams scanParams;


    public RedisServerKeyIterator(Jedis client, String prefix) {
        this.client = client;
        scanParams = new ScanParams().count(SCAN_COUNT_SIZE).match(prefix + "*");
        scanCursor = client.scan(ScanParams.SCAN_POINTER_START_BINARY, scanParams);
        keyResults = scanCursor.getResult();
    }

    public void release() {
        client.close();
    }

    @Override
    public boolean hasNext() {
        if (position < keyResults.size()) {
            return true;
        } else if (!scanCursor.getCursor().equals("0")) {
            scanCursor = client.scan(scanCursor.getCursorAsBytes(), scanParams);
            keyResults = scanCursor.getResult();
            position = 0;

            return keyResults.size() > 0;
        }

        return false;
    }

    @Override
    public byte[] next() {
        return keyResults.get(position++);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
