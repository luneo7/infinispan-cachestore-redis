package org.infinispan.persistence.redis;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisSentinel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "persistence.redis.RedisSentinelStoreParallelIterationTest", groups = "functional")
public class RedisSentinelStoreParallelIterationTest extends ParallelIterationTest
{
    private RedisSentinel redisServer;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void createBeforeClass() throws Exception {
        redisServer = new RedisSentinel();
        redisServer.start();
        super.createBeforeClass();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        redisServer.kill();
    }

    @Override
    protected void configurePersistence(ConfigurationBuilder cb) {
        cb.persistence()
          .addStore(RedisStoreConfigurationBuilder.class)
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
          .port(26381)
          .database(0);
    }
}
