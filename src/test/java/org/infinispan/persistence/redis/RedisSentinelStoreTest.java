package org.infinispan.persistence.redis;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisSentinel;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

@Test(testName = "persistence.redis.RedisServerStoreTest", groups = "functional")
public class RedisSentinelStoreTest extends BaseNonBlockingStoreTest
{
    RedisSentinel redisServer;

    @BeforeClass(alwaysRun = true)
    public void beforeClass()
        throws IOException
    {
        System.out.println("RedisSentinelStoreTest:Setting up");
        redisServer = new RedisSentinel();
        redisServer.start();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        System.out.println("RedisSentinelStoreTest:Tearing down");
        redisServer.kill();
    }

    @Override
    protected NonBlockingStore createStore() throws Exception
    {
        return new RedisStore();
    }

    @Override
    protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
        ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
        RedisStoreConfigurationBuilder storeConfigurationBuilder = builder.persistence().addStore(RedisStoreConfigurationBuilder.class);
        storeConfigurationBuilder
                .topology(Topology.SENTINEL)
                .masterName("mymaster")
                .addSentinel()
                .host("localhost")
                .port(26379)
                .addSentinel()
                .host("localhost")
                .port(26380)
                .addSentinel()
                .host("localhost")
                .port(26381);
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
