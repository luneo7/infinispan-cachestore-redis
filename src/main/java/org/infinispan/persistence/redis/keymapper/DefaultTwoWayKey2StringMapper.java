package org.infinispan.persistence.redis.keymapper;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.WrappedByteArrayOrPrimitiveMapper;
import org.infinispan.persistence.spi.PersistenceException;

import java.io.IOException;
import java.util.Base64;

public class DefaultTwoWayKey2StringMapper extends WrappedByteArrayOrPrimitiveMapper implements MarshallingTwoWayKey2StringMapper {
    private Marshaller marshaller;

    @Override
    public void setMarshaller(StreamingMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public void setMarshaller(Marshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public String getStringMapping(Object key) {
        if (super.isSupportedType(key.getClass())) {
            return super.getStringMapping(key);
        }
        try {
            return Base64.getEncoder().encodeToString(marshaller.objectToByteBuffer(key));
        } catch (IOException | InterruptedException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public Object getKeyMapping(String key) {
        if (super.isSupportedType(key.getClass())) {
            return super.getKeyMapping(key);
        }
        try {
            return marshaller.objectFromByteBuffer(Base64.getDecoder().decode(key));
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public boolean isSupportedType(Class<?> keyType) {
        return super.isSupportedType(keyType) || Object.class.isAssignableFrom(keyType);
    }
}
