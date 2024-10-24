package org.infinispan.persistence.redis;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.redis.configuration.RedisStoreConfiguration.Topology;
import org.infinispan.persistence.redis.configuration.RedisStoreConfigurationBuilder;
import org.infinispan.persistence.redis.support.RedisCluster;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "persistence.redis.RedisClusterStoreParallelIterationTest", groups = "functional")
public class RedisClusterStoreParallelIterationTest extends ParallelIterationTest {
    private RedisCluster redisCluster;

    @Override
    protected void configurePersistence(ConfigurationBuilder cb) {
        cb.persistence()
          .addStore(RedisStoreConfigurationBuilder.class)
          .topology(Topology.CLUSTER)
          .addServer()
          .host("localhost")
          .port(6390);
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void createBeforeClass() throws Exception {
        System.out.println("RedisClusterStoreParallelIterationTest:Setting up");
        redisCluster = new RedisCluster();
        redisCluster.start();
        super.createBeforeClass();
    }

    @AfterClass(alwaysRun = true)
    public void afterClass() {
        System.out.println("RedisClusterStoreParallelIterationTest:Tearing down");
        redisCluster.kill();
    }
}
