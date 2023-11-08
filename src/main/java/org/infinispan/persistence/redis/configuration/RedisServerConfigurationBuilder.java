package org.infinispan.persistence.redis.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

final public class RedisServerConfigurationBuilder extends AbstractRedisStoreConfigurationChildBuilder<RedisStoreConfigurationBuilder> implements
    Builder<RedisServerConfiguration>
{
    private String host;
    private int port = 6379;
    private boolean ssl = false;

    RedisServerConfigurationBuilder(RedisStoreConfigurationBuilder builder)
    {
        super(builder);
    }

    public RedisServerConfigurationBuilder host(String host)
    {
        this.host = host;
        return this;
    }

    public RedisServerConfigurationBuilder port(int port)
    {
        this.port = port;
        return this;
    }

    public RedisServerConfigurationBuilder ssl(boolean ssl)
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
