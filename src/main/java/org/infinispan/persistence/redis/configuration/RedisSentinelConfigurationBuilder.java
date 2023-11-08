package org.infinispan.persistence.redis.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

public class RedisSentinelConfigurationBuilder extends AbstractRedisStoreConfigurationChildBuilder<RedisStoreConfigurationBuilder> implements
    Builder<RedisServerConfiguration>
{
    private String host;
    private int port = 26379;
    private boolean ssl = false;

    protected RedisSentinelConfigurationBuilder(RedisStoreConfigurationBuilder builder)
    {
        super(builder);
    }

    public RedisSentinelConfigurationBuilder host(String host)
    {
        this.host = host;
        return this;
    }

    public RedisSentinelConfigurationBuilder port(int port)
    {
        this.port = port;
        return this;
    }

    public RedisSentinelConfigurationBuilder ssl(boolean ssl)
    {
        this.ssl = ssl;
        return this;
    }

    @Override
    public void validate()
    {

    }

    @Override
    public void validate(GlobalConfiguration globalConfig)
    {

    }

    @Override
    public RedisServerConfiguration create()
    {
        return new RedisServerConfiguration(host, port, ssl);
    }

    @Override
    public Builder<?> read(RedisServerConfiguration template, Combine combine)
    {
        host = template.host();
        port = template.port();
        ssl = template.ssl();

        return this;
    }

    @Override
    public AttributeSet attributes() {
        return AttributeSet.EMPTY;
    }
}
