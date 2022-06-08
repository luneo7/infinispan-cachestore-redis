package org.infinispan.persistence.redis;

import org.infinispan.Cache;
import org.infinispan.persistence.redis.support.RedisServer;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.Assert.assertEquals;

@Test(testName = "persistence.redis.RedisServerStoreConfigTest", groups = "functional")
public class RedisServerStoreConfigTest extends AbstractInfinispanTest
{
    private static final String CACHE_LOADER_CONFIG = "redis-server-cl-config.xml";
    private RedisServer redisServer;

    @BeforeTest(alwaysRun = true)
    public void beforeTest()
        throws IOException
    {
        System.out.println("RedisServerStoreConfigTest:Setting up");
        redisServer = new RedisServer();
        redisServer.start();
    }

    public void simpleTest() throws Exception
    {
        withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
            @Override
            public void call() {
                Cache<Object, Object> cache = cm.getCache();
                NonBlockingStore<Object, Object> store = TestingUtil.getFirstStore(cache);
                assert store != null;
                assert store instanceof RedisStore;

                cache.put("k", "v");

                assertEquals(1, cm.getCache().size());
                cache.stop();
            }
        });
    }

    @AfterTest(alwaysRun = true)
    public void afterTest()
    {
        System.out.println("RedisServerStoreConfigTest:Tearing down");
        this.redisServer.kill();
    }
}
