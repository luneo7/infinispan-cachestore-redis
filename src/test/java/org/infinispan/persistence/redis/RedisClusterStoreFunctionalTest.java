package org.infinispan.persistence.redis;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;

@Test(testName = "persistence.redis.RedisClusterStoreFunctionalTest", groups = "functional")
public class RedisClusterStoreFunctionalTest extends BaseStoreFunctionalTest
{
    private RedisCluster redisCluster;

    @BeforeClass(alwaysRun = true)
    public void beforeClass()
        throws Exception
    {
        System.out.println("RedisClusterStoreFunctionalTest:Setting up");
        redisCluster = new RedisCluster();
        redisCluster.start();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        System.out.println("RedisClusterStoreFunctionalTest:Tearing down");
        redisCluster.kill();
    }

    @Override
    protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
                                                                     String cacheName,
                                                                     boolean preload)
    {
        persistence
            .addStore(RedisStoreConfigurationBuilder.class)
            .topology(Topology.CLUSTER)
            .preload(preload)
            .addServer()
            .host("localhost")
            .port(6390)
        ;

        return persistence;
    }

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
        global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
        global.serialization().addContextInitializer(getSerializationContextInitializer());
        global.cacheContainer().security().authorization().disable();
        return createCacheManager(false, global, new ConfigurationBuilder());
    }

    @Override
    public void testPreloadAndExpiry()
    {
        // No support for pre-load
    }

    @Override
    public void testPreloadStoredAsBinary()
    {
        // No support for pre-load
    }

    @Override
    public void testTwoCachesSameCacheStore()
    {
        // Cluster mode does not support database index selection, and so the cache store cannot
        // support two cache stores using the same clustered Redis backend.
    }

    @Override
    public void testPurgeWithConcurrentUpdate() {
        // This test doesn't work as purgeExpired does nothing
    }
}
