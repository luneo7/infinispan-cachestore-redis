package org.infinispan.persistence.redis;

import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

/**
 * Interface used to initialise the {@link PersistenceMarshallerImpl}'s {@link org.infinispan.protostream.SerializationContext}
 * using the specified Pojos, Marshaller implementations and provided .proto schemas.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@AutoProtoSchemaBuilder(
        includeClasses = {
                RedisStoreConfiguration.Compressor.class,
                RedisStore.Entry.class,
        },
        schemaFileName = "persistence.redis.proto",
        schemaFilePath = "proto/generated",
        schemaPackageName = "org.infinispan.persistence.redis",
        service = false
)
interface PersistenceContextInitializer extends SerializationContextInitializer {
}
