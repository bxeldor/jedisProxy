package redis.clients.jedisProxy.tests;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import cn.sina.dsp.jedisproxy.JedisProxyPool;

import com.google.common.collect.ImmutableList;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

//10.13.8.33
//20001
//weibo_test
public class JedisProxyPoolTest {
	HostAndPort hnp = new HostAndPort("10.73.11.21", 30000);
	ImmutableList<HostAndPort> of = ImmutableList.of(hnp);
	public static JedisPoolConfig config = new JedisPoolConfig();
	JedisProxyPool pool;
    @Before
	public void setUp() throws Exception {
		config.setMaxTotal(5);
		config.setMaxIdle(5);
		config.setMinIdle(3);
	//	pool = new JedisProxyPool(config, of, "liudong_cluster");
	}

	@Test
	public void testReturnResource() {
		JedisProxyPool pool = new JedisProxyPool(of, "liudong_cluster");
		pool.getResource();
		pool.returnResource(pool.getResource());
	}

	@Test
	public void checkConnections() {
		JedisProxyPool pool = new JedisProxyPool(config, of, "liudong_cluster", 2000);
		Jedis jedis = pool.getResource();
		
		jedis.set("foo", "bar");
		assertEquals("bar", jedis.get("foo"));
		jedis.close();
		pool.destroy();
		assertTrue(pool.isClosed());
	}

	@Test
	public void checkCloseableConnections() throws Exception {
		JedisProxyPool pool = new JedisProxyPool(config, of, "liudong_cluster", 2000);
		Jedis jedis = pool.getResource();
		
		jedis.set("foo", "bar");
		assertEquals("bar", jedis.get("foo"));
		jedis.close();
		pool.close();
		assertTrue(pool.isClosed());
	}

	@Test
	public void checkJedisIsReusedWhenReturned() {

		JedisProxyPool pool = new JedisProxyPool(config, of, "liudong_cluster");
		Jedis jedis = pool.getResource();
		
		jedis.set("foo", "0");
		jedis.close();

		jedis = pool.getResource();
		
		jedis.incr("foo");
		jedis.close();
		pool.destroy();
		assertTrue(pool.isClosed());
	}

	@Test
	public void checkPoolRepairedWhenJedisIsBroken() {
		Jedis jedis = pool.getResource();
//		jedis.quit();
		jedis.close();

		jedis = pool.getResource();
		
		jedis.incr("foo");
		jedis.close();
		pool.destroy();
		assertTrue(pool.isClosed());
	}

	@Test
	public void checkPoolOverflow() {

		config.setMaxTotal(1);
		config.setBlockWhenExhausted(false);
		Jedis jedis = pool.getResource();
		
		jedis.set("foo", "0");

		Jedis newJedis = pool.getResource();
		
		newJedis.incr("foo");
	}

	@Test
	public void securePool() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(true);
		Jedis jedis = pool.getResource();
		jedis.set("foo", "bar");
		jedis.close();
		pool.destroy();
		assertTrue(pool.isClosed());
	}

	@Test
	public void returnResourceShouldResetState() {
		config.setMaxTotal(1);
		config.setBlockWhenExhausted(false);

		Jedis jedis = pool.getResource();
		try {
			jedis.set("hello", "jedis");
		} finally {
			jedis.close();
		}

		Jedis jedis2 = pool.getResource();
		try {
			assertTrue(jedis == jedis2);
			assertEquals("jedis", jedis2.get("hello"));
		} finally {
			jedis2.close();
		}

		pool.destroy();
		assertTrue(pool.isClosed());
	}

	@Test
	public void checkResourceIsCloseable() {
		config.setMaxTotal(1);
		config.setBlockWhenExhausted(false);
		Jedis jedis = pool.getResource();
		try {
			jedis.set("hello", "jedis");
		} finally {
			jedis.close();
		}

		Jedis jedis2 = pool.getResource();
		try {
			assertEquals(jedis, jedis2);
		} finally {
			jedis2.close();
		}
	}

	@Test
	public void getNumActiveIsNegativeWhenPoolIsClosed() {
		pool.destroy();
		assertTrue(pool.getNumActive() < 0);
	}

	@Test
	public void getNumActiveReturnsTheCorrectNumber() {
		Jedis jedis = pool.getResource();
		
		jedis.set("foo", "bar");
		assertEquals("bar", jedis.get("foo"));

		assertEquals(1, pool.getNumActive());

		Jedis jedis2 = pool.getResource();
		
		jedis.set("foo", "bar");

		assertEquals(2, pool.getNumActive());

		jedis.close();
		assertEquals(1, pool.getNumActive());

		jedis2.close();

		assertEquals(0, pool.getNumActive());

		pool.destroy();
	}
}
