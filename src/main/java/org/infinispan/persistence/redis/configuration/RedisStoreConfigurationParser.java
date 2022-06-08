package org.infinispan.persistence.redis.configuration;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.CacheParser;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Compressor;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;
import org.kohsuke.MetaInfServices;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.persistence.redis.configuration.RedisStoreConfigurationParser.NAMESPACE;

@MetaInfServices
@Namespaces({
        @Namespace(uri = NAMESPACE + "*", root = "redis-store"),
        @Namespace(root = "redis-store")
})
final public class RedisStoreConfigurationParser implements ConfigurationParser {
    static final String NAMESPACE = Parser.NAMESPACE + "store:redis:";

    @Override
    public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
        ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

        Element element = Element.forName(reader.getLocalName());
        if (element == Element.REDIS_STORE) {
            this.parseRedisStore(reader, builder.persistence());
        } else {
            throw ParseUtils.unexpectedElement(reader);
        }
    }

    private void parseRedisStore(
            ConfigurationReader reader,
            PersistenceConfigurationBuilder persistenceBuilder
    ) {
        RedisStoreConfigurationBuilder builder = new RedisStoreConfigurationBuilder(persistenceBuilder);
        this.parseRedisStoreAttributes(reader, builder);

        while (reader.inTag()) {
            Element element = Element.forName(reader.getLocalName());
            switch (element) {
                case SERVER: {
                    this.parseServer(reader, builder.addServer());
                    break;
                }

                case SENTINEL: {
                    this.parseSentinel(reader, builder.addSentinel());
                    break;
                }

                case CONNECTION_POOL: {
                    this.parseConnectionPool(reader, builder.connectionPool());
                    break;
                }
                default: {
                    CacheParser.parseStoreElement(reader, builder);
                    break;
                }
            }
        }

        persistenceBuilder.addStore(builder);
    }

    private void parseServer(ConfigurationReader reader, RedisServerConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case HOST:
                    builder.host(value);
                    break;

                case PORT:
                    builder.port(Integer.parseInt(value));
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }

        ParseUtils.requireNoContent(reader);
    }

    private void parseSentinel(ConfigurationReader reader, RedisSentinelConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case HOST:
                    builder.host(value);
                    break;

                case PORT:
                    builder.port(Integer.parseInt(value));
                    break;

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }

        ParseUtils.requireNoContent(reader);
    }

    private void parseConnectionPool(ConfigurationReader reader, ConnectionPoolConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case TIME_BETWEEN_EVICTION_RUNS: {
                    builder.timeBetweenEvictionRuns(Long.parseLong(value));
                    break;
                }

                case MIN_EVICTABLE_IDLE_TIME: {
                    builder.minEvictableIdleTime(Long.parseLong(value));
                    break;
                }

                case MAX_TOTAL: {
                    builder.maxTotal(Integer.parseInt(value));
                    break;
                }

                case MAX_IDLE: {
                    builder.maxIdle(Integer.parseInt(value));
                    break;
                }

                case MIN_IDLE: {
                    builder.minIdle(Integer.parseInt(value));
                    break;
                }

                case TEST_ON_CREATE: {
                    builder.testOnCreate(Boolean.parseBoolean(value));
                    break;
                }

                case TEST_ON_BORROW: {
                    builder.testOnBorrow(Boolean.parseBoolean(value));
                    break;
                }

                case TEST_ON_RETURN: {
                    builder.testOnReturn(Boolean.parseBoolean(value));
                    break;
                }

                case TEST_ON_IDLE: {
                    builder.testOnIdle(Boolean.parseBoolean(value));
                    break;
                }

                default:
                    throw ParseUtils.unexpectedAttribute(reader, i);
            }
        }

        ParseUtils.requireNoContent(reader);
    }

    private void parseRedisStoreAttributes(ConfigurationReader reader, RedisStoreConfigurationBuilder builder) {
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            ParseUtils.requireNoNamespaceAttribute(reader, i);
            String value = replaceProperties(reader.getAttributeValue(i));
            Attribute attribute = Attribute.forName(reader.getAttributeName(i));
            switch (attribute) {
                case DATABASE: {
                    builder.database(Integer.parseInt(value));
                    break;
                }

                case PASSWORD: {
                    builder.password(value);
                    break;
                }

                case TOPOLOGY: {
                    builder.topology(Topology.valueOf(value.toUpperCase()));
                    break;
                }

                case SOCKET_TIMEOUT: {
                    builder.socketTimeout(Integer.parseInt(value));
                    break;
                }

                case CONNECTION_TIMEOUT: {
                    builder.connectionTimeout(Integer.parseInt(value));
                    break;
                }

                case MASTER_NAME: {
                    builder.masterName(value);
                    break;
                }

                case MAX_REDIRECTIONS: {
                    builder.maxRedirections(Integer.parseInt(value));
                    break;
                }

                case COMPRESSOR: {
                    builder.compressor(Compressor.valueOf(value.toUpperCase()));
                    break;
                }

                case COMPRESSION_BLOCK_SIZE: {
                    builder.compressionBlockSize(Integer.parseInt(value));
                    break;
                }

                case COMPRESSION_LEVEL: {
                    builder.compressionLevel(Integer.parseInt(value));
                    break;
                }

                case KEY_TO_STRING_MAPPER: {
                    builder.key2StringMapper(value);
                    break;
                }

                default: {
                    CacheParser.parseStoreAttribute(reader, i, builder);
                }
            }
        }
    }

    @Override
    public Namespace[] getNamespaces() {
        return ParseUtils.getNamespaceAnnotations(getClass());
    }
}
