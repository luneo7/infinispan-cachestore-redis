package org.infinispan.persistence.redis;

import io.reactivex.rxjava3.core.Flowable;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.persistence.redis.client.RedisConnection;
import org.infinispan.persistence.redis.client.RedisConnectionPool;
import org.infinispan.persistence.redis.client.RedisConnectionPoolFactory;
import org.infinispan.persistence.redis.compression.Compressor;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration;
import org.infinispan.persistence.redis.keymapper.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import redis.clients.jedis.util.SafeEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.lang.Math.min;
import static org.infinispan.persistence.redis.compression.Util.getCompressor;

@ConfiguredBy(RedisStoreConfiguration.class)
final public class RedisStore<K, V> implements NonBlockingStore<K, V> {
    private static final Log LOGGER = LogFactory.getLog(RedisStore.class, Log.class);

    private volatile RedisConnectionPool connectionPool = null;

    private BlockingManager blockingManager;
    private MarshallableEntryFactory<K, V> entryFactory;
    private KeyPartitioner keyPartitioner;
    private TimeService timeService;
    private String cacheName;
    private Compressor compressor;
    private RedisKeyHandler keyHandler;
    private RedisStoreConfiguration.Compressor defaultCompressor;
    private PersistenceMarshaller persistenceMarshaller;
    private final Map<RedisStoreConfiguration.Compressor, Compressor> decompressors = Collections.synchronizedMap(
            new EnumMap<>(RedisStoreConfiguration.Compressor.class));
    private RedisStoreConfiguration redisStoreConfiguration;

    @Override
    public Set<Characteristic> characteristics() {
        return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SHAREABLE);
    }

    @Override
    public CompletionStage<Void> start(InitializationContext ctx) {
        redisStoreConfiguration = ctx.getConfiguration();
        persistenceMarshaller = ctx.getPersistenceMarshaller();

        entryFactory = ctx.getMarshallableEntryFactory();
        blockingManager = ctx.getBlockingManager();
        keyPartitioner = ctx.getKeyPartitioner();
        timeService = ctx.getTimeService();

        TwoWayKey2StringMapper twoWayKey2StringMapper = getTwoWayMapper(ctx, persistenceMarshaller, redisStoreConfiguration);
        String ctxCacheName = ctx.getCache().getName();

        cacheName = ctxCacheName != null ? ctxCacheName : "";
        compressor = getCompressor(redisStoreConfiguration, persistenceMarshaller);
        defaultCompressor = redisStoreConfiguration.compressor();
        keyHandler = cacheName.isEmpty() ? new KeyHandler(twoWayKey2StringMapper) : new KeyWithCacheNameHandler(cacheName,
                                                                                                                twoWayKey2StringMapper);

        ctx.getPersistenceMarshaller().register(new PersistenceContextInitializerImpl());

        return blockingManager.runBlocking(() -> {
            LOGGER.infof("Starting Redis store for cache '%s'", cacheName);

            try {
                connectionPool = RedisConnectionPoolFactory.factory(redisStoreConfiguration);
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to initialise the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            }
        }, "redis-start");
    }

    @Override
    public CompletionStage<Void> stop() {
        return blockingManager.runBlocking(() -> {
            LOGGER.infof("Stopping Redis store for cache '%s'", cacheName);

            if (null != connectionPool) {
                connectionPool.shutdown();
            }
        }, "redis-stop");
    }

    @Override
    public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
        return blockingManager.supplyBlocking(() -> {
            RedisConnection connection = null;

            try {
                connection = connectionPool.getConnection();

                byte[] keyBytes = keyHandler.marshallKey(segment, key);
                byte[] value = connection.get(keyBytes);

                if (null == value) {
                    return null;
                }

                MarshallableEntry<K, V> entry = unmarshallEntry(key, value);

                if (entry != null && entry.isExpired(timeService.wallClockTime())) {
                    connection.delete(keyBytes);
                    return null;
                }

                return entry;
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to load element from the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-load");
    }

    @Override
    public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
        return blockingManager.runBlocking(() -> {
            RedisConnection connection = null;

            try {
                long lifespan = -1;
                byte[] marshalledKey = keyHandler.marshallKey(segment, entry.getKey());
                byte[] marshalledValue = persistenceMarshaller.objectToByteBuffer(new Entry(marshall(entry.getMarshalledValue()),
                                                                                            defaultCompressor));

                Metadata metadata = entry.getMetadata();

                if (null != metadata) {
                    long lset = metadata.lifespan();
                    long muet = metadata.maxIdle();
                    if (lset <= 0 && muet > 0) {
                        lifespan = toSeconds(muet, entry.getKey());
                    } else if (muet <= 0 && lset > 0) {
                        lifespan = toSeconds(lset, entry.getKey());
                    } else if (muet > 0) {
                        lifespan = toSeconds(min(lset, muet), entry.getKey());
                    }
                }

                connection = connectionPool.getConnection();
                connection.set(marshalledKey, marshalledValue, lifespan);
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to write element to the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-write");
    }

    @Override
    public CompletionStage<Boolean> delete(int segment, Object key) {
        return blockingManager.supplyBlocking(() -> {
            RedisConnection connection = null;
            try {
                connection = connectionPool.getConnection();
                return connection.delete(keyHandler.marshallKey(segment, key));
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to delete element from the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-delete");
    }

    @Override
    public CompletionStage<Void> clear() {
        return blockingManager.runBlocking(() -> {
            RedisConnection connection = null;
            try {
                connection = connectionPool.getConnection();
                connection.flushDb();
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to clear all elements in the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-clear");
    }

    @Override
    public CompletionStage<Boolean> containsKey(int segment, Object key) {
        return blockingManager.supplyBlocking(() -> {
            RedisConnection connection = null;

            try {
                connection = connectionPool.getConnection();
                return connection.exists(keyHandler.marshallKey(segment, key));
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to discover if element is in the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-contains-key");
    }


    @Override
    public CompletionStage<Long> size(IntSet segments) {
        return blockingManager.supplyBlocking(() -> {
            RedisConnection connection = null;
            try {
                connection = connectionPool.getConnection();
                return connection.dbSize();
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to fetch element count from the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-size");
    }

    @Override
    public CompletionStage<Long> approximateSize(IntSet segments) {
        return size(segments);
    }

    @Override
    public CompletionStage<Void> addSegments(IntSet segments) {
        return CompletableFutures.completedNull();
    }

    @Override
    public CompletionStage<Void> removeSegments(IntSet segments) {
        if (segments == null) {
            return clear();
        }

        return blockingManager.runBlocking(() -> {
            RedisConnection connection = null;
            try {
                connection = connectionPool.getConnection();
                for (byte[] keyBytes : connection.scan(cacheName)) {
                    Object key = keyHandler.unmarshallKey(keyBytes);
                    int segment = keyPartitioner.getSegment(key);
                    if (segments.contains(segment)) {
                        connection.delete(keyHandler.marshallKey(segment, key));
                    }
                }
            } catch (Exception ex) {
                LOGGER.errorf(ex, "Failed to remove segments from the redis store for cache '%s'", cacheName);
                throw new PersistenceException(ex);
            } finally {
                if (null != connection) {
                    connection.release();
                }
            }
        }, "redis-remove-segments");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
        Predicate<? super K> combinedFilter = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);

        RedisConnection connection = connectionPool.getConnection();

        return blockingManager.blockingPublisher(
                Flowable.using(
                        () -> connection.scan(cacheName),
                        it -> Flowable.fromIterable(it)
                                      .map(o -> keyHandler.unmarshallKey(o))
                                      .filter(key -> combinedFilter == null || combinedFilter.test((K) key))
                                      .flatMap(key -> Flowable.fromCompletionStage(load(-1, key)))
                                      .filter(Objects::nonNull),
                        it -> connection.release()
                )
        );

    }

    @Override
    public Publisher<MarshallableEntry<K, V>> purgeExpired() {
        return Flowable.empty();
    }

    private int toSeconds(long millis, Object key) {
        if (millis > 0 && millis < 1000) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.tracef(
                        "Adjusting lifespan time for (k,v): (%s, %s) from %d millis to 1 sec, as milliseconds are not supported by Redis",
                        key, millis);
            }

            return 1;
        }

        return (int) TimeUnit.MILLISECONDS.toSeconds(millis);
    }

    private byte[] marshall(Object entry) {
        return compressor.compress(entry);
    }

    private <E> E unmarshall(byte[] bytes, Compressor compressor) {
        return compressor.uncompress(bytes);
    }

    private MarshallableEntry<K, V> unmarshallEntry(Object key, byte[] valueBytes) {
        try {
            Entry entry = (Entry) persistenceMarshaller.objectFromByteBuffer(valueBytes);
            Compressor decompressor = compressor;

            if (entry.compressor != null && defaultCompressor != entry.compressor) {
                decompressor = decompressors.computeIfAbsent(entry.compressor,
                                                             ignored -> getCompressor(entry.compressor, redisStoreConfiguration,
                                                                                      persistenceMarshaller));
            }

            MarshalledValue value = unmarshall(entry.marshalledValue, decompressor);

            if (value == null) {
                return null;
            }

            return entryFactory.create(key,
                                       value.getValueBytes(),
                                       value.getMetadataBytes(),
                                       value.getInternalMetadataBytes(),
                                       value.getCreated(),
                                       value.getLastUsed());
        } catch (IOException | ClassNotFoundException e) {
            throw new PersistenceException(e);
        }

    }

    private TwoWayKey2StringMapper getTwoWayMapper(InitializationContext ctx,
                                                   PersistenceMarshaller persistenceMarshaller,
                                                   RedisStoreConfiguration config) {
        String mapperClass = config.key2StringMapper();
        TwoWayKey2StringMapper twoWayKey2StringMapper = new DefaultTwoWayKey2StringMapper();
        if (mapperClass != null) {
            try {
                twoWayKey2StringMapper = (TwoWayKey2StringMapper) Util.loadClass(mapperClass,
                                                                                 ctx.getGlobalConfiguration()
                                                                                    .classLoader()).newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException("This should not happen.", e);
            }
        }

        if (twoWayKey2StringMapper instanceof MarshallingTwoWayKey2StringMapper) {
            ((MarshallingTwoWayKey2StringMapper) twoWayKey2StringMapper).setMarshaller(persistenceMarshaller);
        }

        return twoWayKey2StringMapper;
    }

    @ProtoTypeId(7777)
    static final class Entry {
        @ProtoField(number = 1)
        byte[] marshalledValue;

        @ProtoField(number = 2)
        RedisStoreConfiguration.Compressor compressor;

        @ProtoFactory
        Entry(byte[] marshalledValue, RedisStoreConfiguration.Compressor compressor) {
            this.marshalledValue = marshalledValue;
            this.compressor = compressor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entry entry)) {
                return false;
            }
            return Arrays.equals(marshalledValue, entry.marshalledValue) && compressor == entry.compressor;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(compressor);
            result = 31 * result + Arrays.hashCode(marshalledValue);
            return result;
        }
    }

    private interface RedisKeyHandler {
        byte[] marshallKey(int segment, Object key);

        <E> E unmarshallKey(byte[] bytes);
    }

    private record KeyWithCacheNameHandler(String cacheName, TwoWayKey2StringMapper twoWayKey2StringMapper) implements RedisKeyHandler {

        @Override
            public byte[] marshallKey(int segment, Object key) {
                return SafeEncoder.encode(cacheName + "|" + twoWayKey2StringMapper.getStringMapping(key));
            }

            @Override
            @SuppressWarnings("unchecked")
            public <E> E unmarshallKey(byte[] bytes) {
                String[] encodedKey = SafeEncoder.encode(bytes).split("\\|");
                int keyPosition = encodedKey.length > 0 ? 1 : 0;
                return (E) twoWayKey2StringMapper.getKeyMapping(encodedKey[keyPosition]);
            }
        }

    private record KeyHandler(TwoWayKey2StringMapper twoWayKey2StringMapper) implements RedisKeyHandler {

        @Override
            public byte[] marshallKey(int segment, Object key) {
                return SafeEncoder.encode(twoWayKey2StringMapper.getStringMapping(key));
            }

            @Override
            @SuppressWarnings("unchecked")
            public <E> E unmarshallKey(byte[] bytes) {
                return (E) twoWayKey2StringMapper.getKeyMapping(SafeEncoder.encode(bytes));
            }
        }
}
