package org.infinispan.persistence.redis.configuration;

final public class RedisServerConfiguration
{
    private final String host;
    private final int port;
    private final boolean ssl;

    RedisServerConfiguration(String host, int port, boolean ssl)
    {
        this.host = host;
        this.port = port;
        this.ssl = ssl;
    }

    public String host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    public boolean ssl()
    {
        return ssl;
    }
}
