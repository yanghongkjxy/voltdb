package org.voltcore.utils;

import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SSLDeferredSerializationIterator implements Iterator<DeferredSerialization> {

    private final SSLEngine sslEngine;
    private final Serializer serializer;
    private Iterator<DeferredSerialization> dsIter;

    public SSLDeferredSerializationIterator(SSLEngine sslEngine, Serializer serializer) {
        this.sslEngine = sslEngine;
        this.serializer = serializer;
    }

    @Override
    public boolean hasNext() {
        // wait to do the serialization until this is called - hence 'deferred'.
        if (dsIter == null) {
            ByteBuffer buf = serializer.serialize();
            List<DeferredSerialization> dsList = new ArrayList<>();
            while (buf.remaining() > 0) {
                if (buf.remaining() < Constants.SSL_CHUNK_SIZE) {
                    ByteBuffer chunk = buf.slice();
                    dsList.add(new SSLDeferredSerialization(sslEngine, chunk));
                    buf.position(buf.limit());
                } else {
                    int oldLimit = buf.limit();
                    int newPosition = buf.position() + Constants.SSL_CHUNK_SIZE;
                    buf.limit(newPosition);
                    ByteBuffer chunk = buf.slice();
                    dsList.add(new SSLDeferredSerialization(sslEngine, chunk));
                    buf.position(newPosition);
                    buf.limit(oldLimit);
                }
            }
            dsIter = dsList.iterator();
        }
        return dsIter.hasNext();
    }

    @Override
    public DeferredSerialization next() {
        return dsIter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
