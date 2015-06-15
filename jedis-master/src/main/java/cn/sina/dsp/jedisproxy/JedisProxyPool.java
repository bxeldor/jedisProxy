package cn.sina.dsp.jedisproxy;

import com.google.common.collect.ImmutableList;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

public class JedisProxyPool extends JedisPoolAbstract{
	
	public JedisProxyPool(final ImmutableList<HostAndPort> hostAndPorts,
			final String clusterKey) {
		this(new JedisPoolConfig(), hostAndPorts, clusterKey);
	}

	public JedisProxyPool(final JedisPoolConfig poolConfig,
			final ImmutableList<HostAndPort> hostAndPorts, final String clusterKey) {
		this(poolConfig, hostAndPorts, clusterKey, Protocol.DEFAULT_TIMEOUT);
	}
	public JedisProxyPool(final JedisPoolConfig poolConfig,
			final ImmutableList<HostAndPort> hostAndPorts, final String clusterKey, final int timeout) {
		this(poolConfig, hostAndPorts, clusterKey,timeout, null);
	}

	public JedisProxyPool(final JedisPoolConfig poolConfig,
			final ImmutableList<HostAndPort> hostAndPorts, final String clusterKey, int timeout, final String password) {
		this(poolConfig, hostAndPorts, clusterKey, timeout, password,  Protocol.DEFAULT_DATABASE);
	}
	public JedisProxyPool(final JedisPoolConfig poolConfig,
			final ImmutableList<HostAndPort> hostAndPorts, final String clusterKey, int timeout, final String password,
			final int database) {
		this(poolConfig, hostAndPorts, clusterKey, timeout, password, database, null);
	}

	public JedisProxyPool(final JedisPoolConfig poolConfig,
			final ImmutableList<HostAndPort> hostAndPorts, final String clusterKey,int timeout, final String password,
			final int database, final String clientName) {
		super(poolConfig, new JedisProxyFactory(hostAndPorts, clusterKey, timeout, password,
				database, clientName));
	}

	@Override
	public Jedis getResource() {
		Jedis jedis = super.getResource();
	//	jedis.setDataSource(this);
		return jedis;
	}

	public void returnBrokenResource(final Jedis resource) {
		if (resource != null) {
			returnBrokenResourceObject(resource);
		}
	}

	public void returnResource(final Jedis resource) {
		if (resource != null) {
			try {
				resource.resetState();
				returnResourceObject(resource);
			} catch (Exception e) {
				returnBrokenResource(resource);
				throw new JedisException(
						"Could not return the resource to the pool", e);
			}
		}
	}
}
