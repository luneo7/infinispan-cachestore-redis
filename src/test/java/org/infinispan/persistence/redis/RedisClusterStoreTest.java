package org.infinispan.persistence.redis;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.BaseStoreTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisCluster;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

@Test(testName = "persistence.redis.RedisClusterStoreTest", groups = "functional")
public class RedisClusterStoreTest extends BaseNonBlockingStoreTest
{
    RedisCluster redisCluster;

    @BeforeClass(alwaysRun = true)
    public void beforeClass()
        throws IOException
    {
        System.out.println("RedisClusterStoreTest:Setting up");
        redisCluster = new RedisCluster();
        redisCluster.start();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        System.out.println("RedisClusterStoreTest:Tearing down");
        redisCluster.kill();
    }

    @Override
    protected NonBlockingStore<Object, Object> createStore() {
        return new RedisStore<>();
    }

    @Override
    protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        RedisStoreConfigurationBuilder storeConfigurationBuilder = builder.persistence().addStore(RedisStoreConfigurationBuilder.class);
        storeConfigurationBuilder
                .topology(Topology.CLUSTER)
                .addServer()
                .host("localhost")
                .port(6390)
                .addServer()
                .host("localhost")
                .port(6391)
                .addServer()
                .host("localhost")
                .port(6392);
        return builder.build();
    }

    @Override
    public void testPreload()
    {
        // No support for pre-load
    }

    @Override
    public void testLoadAndStoreWithIdle() throws Exception
    {
        // No support for idling
    }

    @Override
    public void testLoadAndStoreWithLifespan() throws Exception
    {
        // No support for purge
    }

    @Override
    public void testLoadAndStoreWithLifespanAndIdle() throws Exception
    {
        // No support for purge or idling
    }

    @Override
    public void testLoadAndStoreWithLifespanAndIdle2() throws Exception
    {
        // No support for purge or idling
    }

    @Override
    public void testPurgeExpired() throws Exception
    {
        // No support for purge
    }

    @Override
    public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException
    {
        // No support for advancing time on Redis
    }

    @Override
    public void testReplaceExpiredEntry() throws Exception
    {
        // No support for advancing time on Redis
    }
}
