package org.infinispan.persistence.redis;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "persistence.redis.RedisServerStoreParallelIterationTest", groups = "functional")
public class RedisServerStoreParallelIterationTest extends ParallelIterationTest
{
    private RedisServer redisServer;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void createBeforeClass() throws Exception {
        redisServer = new RedisServer();
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
            .topology(Topology.SERVER)
            .addServer()
            .host("localhost")
            .port(6390)
            .database(0);
    }
}
