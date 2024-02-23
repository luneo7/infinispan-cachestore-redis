package org.infinispan.persistence.redis.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.persistence.redis.RedisStore;
import org.infinispan.persistence.redis.keymapper.DefaultTwoWayKey2StringMapper;
import org.infinispan.protostream.annotations.ProtoEnumValue;

import java.util.ArrayList;
import java.util.List;


@BuiltBy(RedisStoreConfigurationBuilder.class)
@ConfigurationFor(RedisStore.class)
final public class RedisStoreConfiguration extends AbstractStoreConfiguration {
    public enum Topology {
        CLUSTER,
        SENTINEL,
        SERVER
    }

    public enum Compressor {
        @ProtoEnumValue(number = 0, name = "BROTLI")
        BROTLI,
        @ProtoEnumValue(number = 1, name = "GZIP")
        GZIP,
        @ProtoEnumValue(number = 2, name = "LZ4")
        LZ4,
        @ProtoEnumValue(number = 3, name = "LZF")
        LZF,
        @ProtoEnumValue(number = 4, name = "NONE")
        NONE,
        @ProtoEnumValue(number = 5, name = "SNAPPY")
        SNAPPY,
        @ProtoEnumValue(number = 6, name = "ZSTD")
        ZSTD
    }

    static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder("connectionTimeout", 2000).build();
    static final AttributeDefinition<Integer> SOCKET_TIMEOUT = AttributeDefinition.builder("socketTimeout", 2000).build();
    static final AttributeDefinition<String> MASTER_NAME = AttributeDefinition.builder("masterName", null, String.class).build();
    static final AttributeDefinition<Boolean> SSL = AttributeDefinition.builder("ssl", false, Boolean.class).build();
    static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder("password", null, String.class).build();
    static final AttributeDefinition<Integer> DATABASE = AttributeDefinition.builder("database", 0).build();
    static final AttributeDefinition<Integer> MAX_REDIRECTIONS = AttributeDefinition.builder("maxRedirections", 5).build();
    static final AttributeDefinition<Topology> TOPOLOGY = AttributeDefinition.builder("topology", Topology.CLUSTER).build();
    static final AttributeDefinition<Compressor> COMPRESSOR = AttributeDefinition.builder("compressor", Compressor.LZ4).build();
    static final AttributeDefinition<Integer> COMPRESSION_LEVEL = AttributeDefinition.builder("compressionLevel", 1).immutable().build();
    static final AttributeDefinition<Integer> COMPRESSION_BLOCK_SIZE = AttributeDefinition.builder("compressionBlockSize", 32768)
                                                                                          .immutable()
                                                                                          .build();
    static final AttributeDefinition<String> KEY2STRING_MAPPER = AttributeDefinition.builder("key2StringMapper",
                                                                                             DefaultTwoWayKey2StringMapper.class.getName())
                                                                                    .immutable()
                                                                                    .build();

    static final AttributeDefinition<List<RedisServerConfiguration>> SERVERS = AttributeDefinition.builder("servers", null,
                                                                                                           (Class<List<RedisServerConfiguration>>) (Class<?>) List.class)
                                                                                                  .initializer(
                                                                                                          ArrayList::new)
                                                                                                  .build();
    static final AttributeDefinition<List<RedisServerConfiguration>> SENTINELS = AttributeDefinition.builder("sentinels", null,
                                                                                                             (Class<List<RedisServerConfiguration>>) (Class<?>) List.class)
                                                                                                    .initializer(
                                                                                                            ArrayList::new)
                                                                                                    .build();

    public static AttributeSet attributeDefinitionSet() {
        return new AttributeSet(RedisStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(),
                                PASSWORD, DATABASE, SERVERS, SENTINELS, TOPOLOGY, COMPRESSOR, COMPRESSION_BLOCK_SIZE, COMPRESSION_LEVEL,
                                CONNECTION_TIMEOUT, SOCKET_TIMEOUT, MASTER_NAME, MAX_REDIRECTIONS, KEY2STRING_MAPPER, SSL);
    }

    private final ConnectionPoolConfiguration connectionPool;
    private final Attribute<List<RedisServerConfiguration>> servers;
    private final Attribute<List<RedisServerConfiguration>> sentinels;
    private final Attribute<Integer> database;
    private final Attribute<String> password;
    private final Attribute<String> masterName;
    private final Attribute<Topology> topology;
    private final Attribute<Integer> socketTimeout;
    private final Attribute<Integer> connectionTimeout;
    private final Attribute<Integer> maxRedirections;
    private final Attribute<Compressor> compressor;
    private final Attribute<Integer> compressionLevel;
    private final Attribute<Integer> compressionBlockSize;
    private final Attribute<String> key2StringMapper;

    private final Attribute<Boolean> ssl;

    public RedisStoreConfiguration(
            AttributeSet attributes,
            AsyncStoreConfiguration async,
            ConnectionPoolConfiguration connectionPool
    ) {
        super(attributes, async);
        this.connectionPool = connectionPool;
        servers = attributes.attribute(SERVERS);
        sentinels = attributes.attribute(SENTINELS);
        password = attributes.attribute(PASSWORD);
        masterName = attributes.attribute(MASTER_NAME);
        database = attributes.attribute(DATABASE);
        topology = attributes.attribute(TOPOLOGY);
        socketTimeout = attributes.attribute(SOCKET_TIMEOUT);
        connectionTimeout = attributes.attribute(CONNECTION_TIMEOUT);
        maxRedirections = attributes.attribute(MAX_REDIRECTIONS);
        compressor = attributes.attribute(COMPRESSOR);
        compressionBlockSize = attributes.attribute(COMPRESSION_BLOCK_SIZE);
        compressionLevel = attributes.attribute(COMPRESSION_LEVEL);
        key2StringMapper = attributes.attribute(KEY2STRING_MAPPER);
        ssl = attributes.attribute(SSL);
    }

    public List<RedisServerConfiguration> servers() {
        return servers.get();
    }

    public List<RedisServerConfiguration> sentinels() {
        return sentinels.get();
    }

    public int database() {
        return database.get();
    }

    public String password() {
        return password.get();
    }

    public Topology topology() {
        return topology.get();
    }

    public ConnectionPoolConfiguration connectionPool() {
        return connectionPool;
    }

    public int connectionTimeout() {
        return connectionTimeout.get();
    }

    public int socketTimeout() {
        return socketTimeout.get();
    }

    public String masterName() {
        return masterName.get();
    }

    public int maxRedirections() {
        return maxRedirections.get();
    }

    public Compressor compressor() {
        return compressor.get();
    }

    public int compressionBlockSize() {
        return compressionBlockSize.get();
    }

    public int compressionLevel() {
        return compressionLevel.get();
    }

    public String key2StringMapper() {
        return key2StringMapper.get();
    }

    public boolean ssl() {
        return ssl.get();
    }
}
