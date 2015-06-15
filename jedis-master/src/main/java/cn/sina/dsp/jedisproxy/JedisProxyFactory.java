package cn.sina.dsp.jedisproxy;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import com.google.common.collect.ImmutableList;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

/**
 * PoolableObjectFactory custom impl.
 */
class JedisProxyFactory implements PooledObjectFactory<Jedis> {

	private static ImmutableList<HostAndPort> hostAndPorts = ImmutableList.of();

	public static void setHostAndPorts(ImmutableList<HostAndPort> hostAndPorts) {
		JedisProxyFactory.hostAndPorts = hostAndPorts;
	}

	private final String clusterKey;
	private int nextIdx = 0;

	private final int timeout;
	private final String password;
	private final String clientName;

	public JedisProxyFactory(ImmutableList<HostAndPort> hostAndPorts,
			final String ClusterKey, final int timeout, final String password,
			final int database) {
		this(hostAndPorts, ClusterKey, timeout, password, database, null);
	}

	public JedisProxyFactory(ImmutableList<HostAndPort> hostAndPorts,
			final String clusterKey, final int timeout, final String password,
			final int database, final String clientName) {
		JedisProxyFactory.hostAndPorts = hostAndPorts;
		this.clusterKey = clusterKey;
		this.timeout = timeout;
		this.password = password;
		this.clientName = clientName;
	}

	@Override
	public void activateObject(PooledObject<Jedis> pooledJedis)
			throws Exception {
		final BinaryJedis jedis = pooledJedis.getObject();
		jedis.ping();
	}

	@Override
	public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
		final BinaryJedis jedis = pooledJedis.getObject();
		if (jedis.isConnected()) {
			try {
				try {
					jedis.quit();
				} catch (Exception e) {
				}
				jedis.disconnect();
			} catch (Exception e) {

			}
		}

	}

	public HostAndPort getOnehostAndPort() {
		HostAndPort hostAndPort = null;
		try {
			if (hostAndPorts == null) {
				return null;
			}
			if (hostAndPorts.size() > 1) {
				int pos = nextIdx;
				hostAndPort = hostAndPorts.get(pos);
				nextIdx = pos >= hostAndPorts.size() - 1 ? 0 : pos + 1;
			} else if (hostAndPorts.size() == 1) {
				hostAndPort = hostAndPorts.get(0);
			}
		} catch (Exception e) {
		}
		JedisProxy.log.info("ip地址：" + hostAndPort.getHost() + ", "
				+ hostAndPort.getPort());
		return hostAndPort;
	}

	@Override
	public PooledObject<Jedis> makeObject() throws Exception {
		final HostAndPort hostAndPort = this.getOnehostAndPort();
		final Jedis jedis = new Jedis(hostAndPort.getHost(),
				hostAndPort.getPort(), this.timeout);
		long start = System.currentTimeMillis();
		//System.out.println("start" + start);
		jedis.connect();
		//System.out.println("value" + (System.currentTimeMillis() - start));
		/*
		 * if (null != this.password) { jedis.auth(this.password); }
		 */
		if (clusterKey != null) {
			jedis.auth(clusterKey);
		}
		if (clientName != null) {
			jedis.clientSetname(clientName);
		}

		return new DefaultPooledObject<Jedis>(jedis);
	}

	@Override
	public void passivateObject(PooledObject<Jedis> pooledJedis)
			throws Exception {
		// TODO maybe should select db 0? Not sure right now.
	}

	@Override
	public boolean validateObject(PooledObject<Jedis> pooledJedis) {
		final BinaryJedis jedis = pooledJedis.getObject();
		try {
			HostAndPort hostAndPort = this.getOnehostAndPort();

			String connectionHost = jedis.getClient().getHost();
			int connectionPort = jedis.getClient().getPort();

			return hostAndPort.getHost().equals(connectionHost)
					&& hostAndPort.getPort() == connectionPort
					&& jedis.isConnected() && jedis.ping().equals("PONG");
		} catch (final Exception e) {
			return false;
		}
	}
}