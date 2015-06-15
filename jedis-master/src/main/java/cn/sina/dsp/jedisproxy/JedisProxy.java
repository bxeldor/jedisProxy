package cn.sina.dsp.jedisproxy;

import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import com.google.common.collect.ImmutableList;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * @author bxeldor
 */
public class JedisProxy implements JedisProxyClient {
	static {
		PropertyConfigurator.configure("log4j.properties");
	}
	public static org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger("jedisProxy");
	private volatile ImmutableList<HostAndPort> hostAndPorts = ImmutableList
			.of();

	static final String REDIS_RET_OK = "OK";
	static final int REDIS_SLOW_TIME = 50;
	private static int max_try_time = 2;
	private static final int DEFAULT_WRITE_RETRY = 1;
	private static final int timeBetweenSync = 60000;

	private Thread syncThread;
	JedisPoolConfig poolConfig;
	JedisProxyPool conn;
	String server;
	int resource;
	int port;

	boolean throwJedisException = false;
	boolean readOnly = false;

	public static final Long readOnlyDefaultLongValue = -1L;
	public static final Double readOnlyDefaultDoubleValue = -1.0;
	public static final Boolean readOnlyDefaultBooleanValue = false;

	private String md5;
	private boolean reWriteConfig = false;
	private String appKeyProxyInfo;
	private String appKeyMD5;

	private static Jedis jedisConfig;
	private static String ret;

	public JedisProxy(final JedisPoolConfig poolConfig,
			final String clusterKey, final String appKey, final String ip,
			final int port) {
		this.poolConfig = poolConfig;
		this.appKeyProxyInfo = appKey + "_proxyinfo";
		this.appKeyMD5 = appKey + "_proxy_md5";
		initPool(poolConfig, clusterKey, appKey, ip, port);
	}

	private void getHostAndPortsFromConfig() {
		try {
			String tempIps = jedisConfig.get(appKeyProxyInfo);
			if (null == tempIps) {
				JedisProxy.log.error("获取Proxyinfo为空");
				reWriteConfig = false;
			} else {
				String[] ips = tempIps.split(",");
				ImmutableList.Builder<HostAndPort> builder = ImmutableList
						.builder();
				for (String tmpIps : ips) {
					String[] tmp = tmpIps.split(":");
					builder.add(new HostAndPort(tmp[0], Integer
							.parseInt(tmp[1])));
				}
				hostAndPorts = builder.build();
				JedisProxyFactory.setHostAndPorts(builder.build());
				md5 = ret;
				reWriteConfig = true;
			}
		} catch (Exception e) {
			JedisProxy.log.error("获取Proxyinfo出错");
		}

	}

	// 拉取md5信息
	private boolean SyncConfigMD5() {
		ret = jedisConfig.get(appKeyMD5);
		if (null != md5 && md5.equals(ret))
			return false;
		// else md5 = ret;
		return true;
	}

	// 初始化连接池
	public void initPool(final JedisPoolConfig poolConfig,
			final String clusterKey, final String appKey, final String ip,
			final int port) {
		jedisConfig = new Jedis(ip, port);
		getHostAndPortsFromConfig();
		SyncConfigMD5();
		this.conn = new JedisProxyPool(poolConfig, hostAndPorts, clusterKey);

		if (timeBetweenSync > 0) {
			syncThread = new Thread(appKey + "-SyncConfigIps") {
				public void run() {
					while (true) {
						try {
							if (SyncConfigMD5())
								getHostAndPortsFromConfig();
							try {
								Thread.sleep(timeBetweenSync);
							} catch (InterruptedException e) {
								log.error(e.getMessage());
							}

						} catch (Exception e) {
							// do nothing
						}
					}
				}
			};
			syncThread.setDaemon(true);
			syncThread.start();
		}

	}

	public boolean isThrowJedisException() {
		return throwJedisException;
	}

	public void setThrowJedisException(boolean throwJedisException) {
		this.throwJedisException = throwJedisException;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public synchronized void close() {
		if (this.conn != null) {
			this.conn.destroy();
		}
	}

	public boolean expire(final String key, final int seconds) {
		if (readOnly) {
			return false;
		}
		Long result = callable(new JedisPorxyCallback<Long>("expire", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.expire(key, seconds);
			}
		});
		if (result == null || result != 1) {
			return false;
		}
		return true;
	}

	@Override
	public boolean expireAt(final String key, final long unixTime) {
		if (readOnly) {
			return false;
		}
		Long result = callable(new JedisPorxyCallback<Long>("expireat", key,
				true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.expireAt(key, unixTime);
			}
		});
		if (result == null || result != 1) {
			return false;
		}
		return true;
	}

	@Override
	public Long ttl(final String key) {
		return callable(new JedisPorxyCallback<Long>("ttl", key, false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.ttl(key);
			}
		});
	}

	@Override
	public boolean persist(final String key) {
		if (readOnly) {
			return false;
		}
		Long result = callable(new JedisPorxyCallback<Long>("persist", key,
				true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.persist(key);
			}
		});
		if (result == null || result != 1) {
			return false;
		}
		return true;
	}

	/**
	 * please be careful to call this
	 */
	public boolean flush() {
		String result = callable(new JedisPorxyCallback<String>("flush", "",
				false) {
			@Override
			public String call(Jedis jedis) {
				return jedis.flushDB();
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public List<String> mget(final List<String> keys) {
		return callable(new JedisPorxyCallback<List<String>>("mget",
				keys.toString(), false) {
			@Override
			public List<String> call(Jedis jedis) {
				return jedis.mget(keys.toArray(new String[keys.size()]));
			}
		});
	}

	public List<String> hmget(final String key, final List<String> fields) {
		return callable(new JedisPorxyCallback<List<String>>("hmget", key,
				false) {
			@Override
			public List<String> call(Jedis jedis) {
				return jedis.hmget(key,
						fields.toArray(new String[fields.size()]));
			}
		});
	}

	/**
	 * pipeline
	 * 
	 * @param keys
	 *            List of List size 2 : id, field
	 * @return
	 */
	public List<String> hmget(final List<List<String>> keys) {

		return hmgetPipeline(keys);
	}

	/**
	 * pipeline
	 * 
	 * @param keys
	 *            List of List size 2 : id, field
	 * @return
	 */
	public List<String> hmgetPipeline(final List<List<String>> keys) {
		return callable(new JedisPorxyCallback<List<String>>("hmgetPipeline",
				keys.toString(), false) {
			@Override
			public List<String> call(Jedis jedis) {
				List<String> values = new ArrayList<String>();
				Pipeline pipeline = jedis.pipelined();

				for (List<String> pair : keys) {
					final String id = pair.get(0);
					final String field = pair.get(1);
					pipeline.hget(id, field);
				}

				List<Object> result = pipeline.syncAndReturnAll();
				for (Object o : result) {
					if (o != null) {
						values.add((String) o);
					} else {
						values.add(null);
					}
				}
				return values;
			}
		}, max_try_time, true);
	}

	public String get(final String key) {
		return callable(new JedisPorxyCallback<String>("get", key, false) {
			@Override
			public String call(Jedis jedis) {
				return jedis.get(key);
			}
		});
	}

	public String hget(final String key, final String field) {
		return callable(new JedisPorxyCallback<String>("hget", key, false) {
			@Override
			public String call(Jedis jedis) {
				return jedis.hget(key, field);
			}
		});
	}

	public Boolean set(final String key, final String value) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("set", key,
				true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.set(key, value);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Long hset(final String key, final String field, final String value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("hset", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hset(key, field, value);
			}
		});
	}

	/**
	 * delete multi fields in a hash
	 * 
	 * @param
	 * @return
	 */
	public List<Long> hmdelete(final String key, final List<String> fields) {
		if (readOnly) {
			return new ArrayList<Long>();
		}
		return callable(new JedisPorxyCallback<List<Long>>("hmdelete", key,
				true) {
			@Override
			public List<Long> call(Jedis jedis) {
				List<Long> values = new ArrayList<Long>();
				Pipeline pipeline = jedis.pipelined();

				for (String field : fields) {
					pipeline.hdel(key, field);
				}
				List<Object> result = pipeline.syncAndReturnAll();
				for (Object o : result) {
					if (o != null) {
						if (o instanceof Long) {
							values.add((Long) o);
						} else {
							values.add(Long.parseLong((String) o));
						}
					} else {
						values.add(null);
					}
				}
				return values;
			}
		}, max_try_time, true);
	}

	@Deprecated
	public Boolean del(final String key) {
		if (readOnly) {
			return false;
		}
		return callable(new JedisPorxyCallback<Boolean>("del", key, true) {
			@Override
			public Boolean call(Jedis jedis) {
				jedis.del(key);
				return true;
			}
		});
	}

	public Boolean hdel(final String key, final String field) {
		if (readOnly) {
			return false;
		}

		byte[] bkey = CodecHandler.encode(key);
		byte[] bfield = CodecHandler.encode(field);
		return hdel(bkey, bfield);
	}

	public Long incr(final String key) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}

		if (key == null) {
			return null;
		}
		return callable(new JedisPorxyCallback<Long>("incr", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.incr(key);
			}
		}, 1);
	}

	public Long hincrBy(final String key, final String field, final long value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("hincrBy", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hincrBy(key, field, value);
			}
		}, 1);
	}

	public Long decr(final String key) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}

		if (key == null) {
			return null;
		}
		return callable(new JedisPorxyCallback<Long>("decr", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.decr(key);
			}
		}, 1);
	}

	public Set<String> hkeys(final String key) {
		return callable(new JedisPorxyCallback<Set<String>>("hkeys", key, false) {
			@Override
			public Set<String> call(Jedis jedis) {
				return jedis.hkeys(key);
			}
		});
	}

	public List<String> hvals(final String key) {
		return callable(new JedisPorxyCallback<List<String>>("hvals", key,
				false) {
			@Override
			public List<String> call(Jedis jedis) {
				return new ArrayList<String>(jedis.hvals(key));
			}
		});
	}

	public Map<String, String> hgetAll(final String key) {
		return callable(new JedisPorxyCallback<Map<String, String>>("hgetAll",
				key, false) {
			@Override
			public Map<String, String> call(Jedis jedis) {
				return jedis.hgetAll(key);
			}
		}, max_try_time, true);
	}

	public Boolean hdelAll(final String key) {
		if (readOnly) {
			return false;
		}

		return del(key);
	}

	public Long hlen(final String key) {
		return callable(new JedisPorxyCallback<Long>("hlen", key, false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hlen(key);
			}
		});
	}

	public Boolean hexists(final String key, final String field) {
		return callable(new JedisPorxyCallback<Boolean>("hexists", key, false) {
			@Override
			public Boolean call(Jedis jedis) {
				return jedis.hexists(key, field);
			}
		});
	}

	/**
	 * delete multi fields in a hash
	 * 
	 * @param
	 * @return
	 */
	public List<Boolean> hmexists(final String key, final List<String> fields) {
		return callable(new JedisPorxyCallback<List<Boolean>>("hmexists", key,
				false) {
			@Override
			public List<Boolean> call(Jedis jedis) {
				List<Boolean> values = new ArrayList<Boolean>();
				Pipeline pipeline = jedis.pipelined();

				for (String field : fields) {
					pipeline.hexists(key, field);
				}

				List<Object> result = pipeline.syncAndReturnAll();
				for (Object o : result) {
					if (o != null) {
						values.add((Boolean) o);
					} else {
						values.add(null);
					}
				}
				return values;
			}
		}, max_try_time, true);
	}

	public Boolean set(final byte[] key, final byte[] value) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("set",
				CodecHandler.toStr(key), true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.set(key, value);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Boolean set(String key, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return set(bkey, value);
	}

	public Boolean set(String key, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return set(key, bvalue);
	}

	public <T extends Serializable> Boolean set(String key, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return set(key, bvalue);
	}

	@Override
	public Boolean setex(final String key, final int seconds, final String value) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("setex", key,
				true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.setex(key, seconds, value);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public byte[] get(final byte[] key) {
		return callable(new JedisPorxyCallback<byte[]>("get",
				CodecHandler.toStr(key), false) {
			@Override
			public byte[] call(Jedis jedis) {
				return jedis.get(key);
			}
		});
	}

	public Boolean exists(final byte[] key) {
		return callable(new JedisPorxyCallback<Boolean>("exists",
				CodecHandler.toStr(key), false) {
			@Override
			public Boolean call(Jedis jedis) {
				return jedis.exists(key);
			}
		});
	}

	public Boolean exists(final String key) {
		byte[] bkey = CodecHandler.encode(key);
		return exists(bkey);
	}

	public Long del(final byte[]... keys) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("del", CodecHandler
				.toString(keys).toString(), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.del(keys);
			}
		});
	}

	public Long del(String... keys) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}

		final byte[][] bkeys = new byte[keys.length][];
		for (int i = 0; i < bkeys.length; i++) {
			bkeys[i] = CodecHandler.encode(keys[i]);
		}
		return del(bkeys);
	}

	public List<byte[]> mget(final byte[]... keys) {
		return callable(new JedisPorxyCallback<List<byte[]>>("mget",
				CodecHandler.toString(keys).toString(), false) {
			@Override
			public List<byte[]> call(Jedis jedis) {
				return jedis.mget(keys);
			}
		});
	}

	@Deprecated
	public Map<String, String> mgetMap(String... keys) {
		List<String> ret = mget(keys);
		Map<String, String> map = null;
		if (ret != null) {
			map = new HashMap<String, String>();
			for (int i = 0; i < keys.length; i++) {
				map.put(keys[i], ret.get(i));
			}
		}
		return map;
	}

	public Boolean mset(final byte[]... keysvalues) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("mset",
				CodecHandler.toString(keysvalues).toString(), true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.mset(keysvalues);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Boolean mset(String... keysvalues) {
		final byte[][] bkeysvalues = new byte[keysvalues.length][];
		for (int i = 0; i < keysvalues.length; i++) {
			bkeysvalues[i] = CodecHandler.encode(keysvalues[i]);
		}
		return mset(bkeysvalues);
	}

	public Boolean mset(Map<byte[], byte[]> keyValueMap) {
		byte[][] bkeysvalues = new byte[keyValueMap.size() * 2][];
		int i = 0;
		for (Entry<byte[], byte[]> e : keyValueMap.entrySet()) {
			bkeysvalues[i++] = e.getKey();
			bkeysvalues[i++] = e.getValue();
		}
		return mset(bkeysvalues);
	}

	public Long decrBy(final byte[] key, final long integer) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(
				new JedisPorxyCallback<Long>("decrBy", CodecHandler.toStr(key),
						true) {
					@Override
					public Long call(Jedis jedis) {
						return jedis.decrBy(key, integer);
					}
				}, 1);
	}

	public Long decrBy(String key, long integer) {
		byte[] bkey = CodecHandler.encode(key);
		return decrBy(bkey, integer);
	}

	public Long decr(final byte[] key) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(
				new JedisPorxyCallback<Long>("decr", CodecHandler.toStr(key),
						true) {
					@Override
					public Long call(Jedis jedis) {
						return jedis.decr(key);
					}
				}, 1);
	}

	public Long incrBy(final byte[] key, final long integer) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}

		return callable(
				new JedisPorxyCallback<Long>("incrBy", CodecHandler.toStr(key),
						true) {
					@Override
					public Long call(Jedis jedis) {
						return jedis.incrBy(key, integer);
					}
				}, 1);
	}

	public Long incrBy(String key, long integer) {
		byte[] bkey = CodecHandler.encode(key);
		return incrBy(bkey, integer);
	}

	public Long incr(final byte[] key) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}

		return callable(
				new JedisPorxyCallback<Long>("incr", CodecHandler.toStr(key),
						true) {
					@Override
					public Long call(Jedis jedis) {
						return jedis.incr(key);
					}
				}, 1);
	}

	public Long hset(final byte[] key, final byte[] field, final byte[] value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("hset",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hset(key, field, value);
			}
		});
	}

	public Long hset(String key, String field, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		byte[] bfield = CodecHandler.encode(field);
		return hset(bkey, bfield, value);
	}

	public Long hset(String key, String field, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return hset(key, field, bvalue);
	}

	public <T extends Serializable> Long hset(String key, String field, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return hset(key, field, bvalue);
	}

	public byte[] hget(final byte[] key, final byte[] field) {
		return callable(new JedisPorxyCallback<byte[]>("hget",
				CodecHandler.toStr(key), false) {
			@Override
			public byte[] call(Jedis jedis) {
				return jedis.hget(key, field);
			}
		});
	}

	public Boolean hmset(final byte[] key, final Map<byte[], byte[]> keyValueMap) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("hmset",
				CodecHandler.toStr(key), true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.hmset(key, keyValueMap);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Boolean hmset(String key, Map<byte[], byte[]> keyValueMap) {
		byte[] bkey = CodecHandler.encode(key);
		return hmset(bkey, keyValueMap);
	}

	public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
		return callable(new JedisPorxyCallback<List<byte[]>>("hmget",
				CodecHandler.toStr(key), false) {
			@Override
			public List<byte[]> call(Jedis jedis) {
				return jedis.hmget(key, fields);
			}
		}, max_try_time, true);
	}

	public List<String> hmget(final String key, final String... fields) {
		return callable(new JedisPorxyCallback<List<String>>("hmget", key,
				false) {
			@Override
			public List<String> call(Jedis jedis) {
				return jedis.hmget(key, fields);
			}
		}, max_try_time, true);
	}

	public Long hincrBy(byte[] key, byte[] field, long value) {
		String skey = CodecHandler.toStr(key);
		String sfield = CodecHandler.toStr(field);
		return hincrBy(skey, sfield, value);
	}

	public Boolean hdel(final byte[] bkey, final byte[] bfield) {
		if (readOnly) {
			return false;
		}
		callable(new JedisPorxyCallback<Long>("hdel", CodecHandler.toStr(bkey),
				true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hdel(bkey, bfield);
			}
		});
		return true;

	}

	public Long hlen(final byte[] key) {
		return callable(new JedisPorxyCallback<Long>("hlen",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.hlen(key);
			}
		});
	}

	public Set<byte[]> hkeys(final byte[] key) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("hkeys",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.hkeys(key);
			}
		}, max_try_time, true);
	}

	public List<byte[]> hvals(final byte[] key) {
		return callable(new JedisPorxyCallback<List<byte[]>>("hvals",
				CodecHandler.toStr(key), false) {
			@Override
			public List<byte[]> call(Jedis jedis) {
				return jedis.hvals(key);
			}
		});
	}

	public Map<byte[], byte[]> hgetAll(final byte[] key) {
		return callable(new JedisPorxyCallback<Map<byte[], byte[]>>("hgetAll",
				CodecHandler.toStr(key), false) {
			@Override
			public Map<byte[], byte[]> call(Jedis jedis) {
				return jedis.hgetAll(key);
			}
		}, max_try_time, true);
	}

	public Long rpush(final byte[] key, final byte[] value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("rpush",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.rpush(key, value);
			}
		});
	}

	public Long rpush(final byte[] key, final byte[]... values) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("rpush",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.rpush(key, values);
			}
		});
	}

	public Long rpush(String key, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return rpush(bkey, value);
	}

	public Long rpush(String key, byte[]... values) {
		byte[] bkey = CodecHandler.encode(key);
		return rpush(bkey, values);
	}

	public Long rpush(String key, String value) {
		byte[] bvalue = CodecHandler.encode(value);
		return rpush(key, bvalue);
	}

	public Long rpush(String key, String... values) {
		byte[][] bvalues = new byte[values.length][];

		for (int i = 0; i < values.length; i++) {
			bvalues[i] = CodecHandler.encode(values[i]);
		}
		return rpush(key, bvalues);
	}

	public Long rpush(String key, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return rpush(key, bvalue);
	}

	public <T extends Serializable> Long rpush(String key, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return rpush(key, bvalue);
	}

	public Long lpush(final byte[] key, final byte[] value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("lpush",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.lpush(key, value);
			}
		});
	}

	public Long lpush(final byte[] key, final byte[]... values) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("lpush",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.lpush(key, values);
			}
		});
	}

	public Long lpush(String key, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return lpush(bkey, value);
	}

	public Long lpush(String key, byte[]... values) {
		byte[] bkey = CodecHandler.encode(key);
		return lpush(bkey, values);
	}

	public Long lpush(String key, String value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lpush(key, bvalue);
	}

	public Long lpush(String key, String... values) {
		byte[][] bvalues = new byte[values.length][];

		for (int i = 0; i < values.length; i++) {
			bvalues[i] = CodecHandler.encode(values[i]);
		}
		return lpush(key, bvalues);
	}

	public Long lpush(String key, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lpush(key, bvalue);
	}

	public <T extends Serializable> Long lpush(String key, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lpush(key, bvalue);
	}

	public Long llen(final byte[] key) {
		return callable(new JedisPorxyCallback<Long>("llen",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.llen(key);
			}
		});
	}

	public Long llen(String key) {
		byte[] bkey = CodecHandler.encode(key);
		return llen(bkey);
	}

	public List<byte[]> lrange(final byte[] key, final int start, final int end) {
		return callable(new JedisPorxyCallback<List<byte[]>>("lrange",
				CodecHandler.toStr(key), false) {
			@Override
			public List<byte[]> call(Jedis jedis) {
				return jedis.lrange(key, start, end);
			}
		});
	}

	public List<String> lrange(final String key, final int start, final int end) {
		return callable(new JedisPorxyCallback<List<String>>("lrange", key,
				false) {
			@Override
			public List<String> call(Jedis jedis) {
				return jedis.lrange(key, start, end);
			}
		});
	}

	public Boolean ltrim(final byte[] key, final int start, final int end) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("ltrim",
				CodecHandler.toStr(key), true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.ltrim(key, start, end);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Boolean ltrim(final String key, final int start, final int end) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("ltrim", key,
				true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.ltrim(key, start, end);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public byte[] lindex(final byte[] key, final int index) {
		return callable(new JedisPorxyCallback<byte[]>("lindex",
				CodecHandler.toStr(key), false) {
			@Override
			public byte[] call(Jedis jedis) {
				return jedis.lindex(key, index);
			}
		});
	}

	public String lindex(final String key, final int index) {
		return callable(new JedisPorxyCallback<String>("lindex", key, false) {
			@Override
			public String call(Jedis jedis) {
				return jedis.lindex(key, index);
			}
		});
	}

	public Boolean lset(final byte[] key, final int index, final byte[] value) {
		if (readOnly) {
			return false;
		}
		String result = callable(new JedisPorxyCallback<String>("lset",
				CodecHandler.toStr(key), true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.lset(key, index, value);
			}
		});
		return REDIS_RET_OK.equalsIgnoreCase(result);
	}

	public Boolean lset(String key, int index, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return lset(bkey, index, value);
	}

	public Boolean lset(String key, int index, String value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lset(key, index, bvalue);
	}

	public Boolean lset(String key, int index, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lset(key, index, bvalue);
	}

	public <T extends Serializable> Boolean lset(String key, int index, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lset(key, index, bvalue);
	}

	public Long lrem(final byte[] key, final int count, final byte[] value) {
		return callable(new JedisPorxyCallback<Long>("lrem",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.lrem(key, count, value);
			}
		});
	}

	public Long lrem(String key, int count, byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return lrem(bkey, count, value);
	}

	public Long lrem(String key, int count, String value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lrem(key, count, bvalue);
	}

	public Long lrem(String key, int count, Number value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lrem(key, count, bvalue);
	}

	public <T extends Serializable> Long lrem(String key, int count, T value) {
		byte[] bvalue = CodecHandler.encode(value);
		return lrem(key, count, bvalue);
	}

	public String flushDB() {
		if (readOnly) {
			return null;
		}
		return callable(new JedisPorxyCallback<String>("flushDB", "", false) {
			@Override
			public String call(Jedis jedis) {
				return jedis.flushDB();
			}
		});
	}

	@Override
	public List<String> mget(final String... keys) {
		return callable(new JedisPorxyCallback<List<String>>("mget",
				Arrays.toString(keys), false) {
			@Override
			public List<String> call(Jedis jedis) {
				return jedis.mget(keys);
			}
		});
	}

	/**
	 * ================================================ methods for sorted set
	 * zset ================================================
	 * */
	@Override
	public Long zadd(final byte[] key, final double score, final byte[] member) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("zadd",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zadd(key, score, member);
			}
		});
	}

	public Long zadd(final String key, final double score, final String member) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("zadd", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zadd(key, score, member);
			}
		});
	}

	@Override
	public Long zrem(final byte[] key, final byte[] member) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("zrem",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zrem(key, member);
			}
		});
	}

	@Override
	public Long zremrangeByRank(final byte[] key, final int start, final int end) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("zremrangeByRank",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zremrangeByRank(key, start, end);
			}
		});
	}

	@Override
	public Long zremrangeByScore(final byte[] key, final double start,
			final double end) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("zremrangeByScore",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zremrangeByScore(key, start, end);
			}
		});
	}

	@Override
	public Double zincrby(final byte[] key, final double score,
			final byte[] member) {
		if (readOnly) {
			return readOnlyDefaultDoubleValue;
		}
		return callable(
				new JedisPorxyCallback<Double>("zincrby",
						CodecHandler.toStr(key), true) {
					@Override
					public Double call(Jedis jedis) {
						return jedis.zincrby(key, score, member);
					}
				}, 1);
	}

	@Override
	public Set<byte[]> zrange(final byte[] key, final int start, final int end) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("zrange",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.zrange(key, start, end);
			}
		});
	}

	@Override
	public Long zrank(final byte[] key, final byte[] member) {
		return callable(new JedisPorxyCallback<Long>("zrank",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zrank(key, member);
			}
		});
	}

	@Override
	public Long zrevrank(final byte[] key, final byte[] member) {
		return callable(new JedisPorxyCallback<Long>("zrevrank",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zrevrank(key, member);
			}
		});
	}

	@Override
	public Set<byte[]> zrevrange(final byte[] key, final int start,
			final int end) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("zrevrange",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.zrevrange(key, start, end);
			}
		});
	}

	@Override
	public Set<Tuple> zrangeWithScores(final byte[] key, final int start,
			final int end) {
		return callable(new JedisPorxyCallback<Set<Tuple>>("zrangeWithScores",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<Tuple> call(Jedis jedis) {
				return jedis.zrangeWithScores(key, start, end);
			}
		});
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(final byte[] key, final int start,
			final int end) {
		return callable(new JedisPorxyCallback<Set<Tuple>>(
				"zrevrangeWithScores", CodecHandler.toStr(key), false) {
			@Override
			public Set<Tuple> call(Jedis jedis) {
				return jedis.zrevrangeWithScores(key, start, end);
			}
		});
	}

	@Override
	public Long zcard(final byte[] key) {
		return callable(new JedisPorxyCallback<Long>("zcard",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zcard(key);
			}
		});
	}

	@Override
	public Double zscore(final byte[] key, final byte[] member) {
		return callable(new JedisPorxyCallback<Double>("zscore",
				CodecHandler.toStr(key), false) {
			@Override
			public Double call(Jedis jedis) {
				return jedis.zscore(key, member);
			}
		});
	}

	@Override
	public Long zcount(final byte[] key, final double min, final double max) {
		return callable(new JedisPorxyCallback<Long>("zcount",
				CodecHandler.toStr(key), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.zcount(key, min, max);
			}
		});
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min,
			final double max) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("zrangeByScore",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.zrangeByScore(key, min, max);
			}
		});
	}

	@Override
	public Set<byte[]> zrangeByScore(final byte[] key, final double min,
			final double max, final int offset, final int count) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("zrangeByScore",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.zrangeByScore(key, min, max, offset, count);
			}
		});
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
			final double min, final double max) {
		return callable(new JedisPorxyCallback<Set<Tuple>>(
				"zrangeByScoreWithScores", CodecHandler.toStr(key), false) {
			@Override
			public Set<Tuple> call(Jedis jedis) {
				return jedis.zrangeByScoreWithScores(key, min, max);
			}
		});
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(final byte[] key,
			final double min, final double max, final int offset,
			final int count) {
		return callable(new JedisPorxyCallback<Set<Tuple>>(
				"zrangeByScoreWithScores", CodecHandler.toStr(key), false) {
			@Override
			public Set<Tuple> call(Jedis jedis) {
				return jedis.zrangeByScoreWithScores(key, min, max, offset,
						count);
			}
		});
	}

	@Override
	public Object evalsha(final String sha1, final int keyCount,
			final String... keys) {
		return callable(new JedisPorxyCallback<Object>("evalsha", sha1, false) {
			@Override
			public Object call(Jedis jedis) {
				return jedis.evalsha(sha1, keyCount, keys);
			}
		}, 1);
	}

	@Override
	public String scriptLoad(final String script) {
		return callable(new JedisPorxyCallback<String>("scriptLoad", "", true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.scriptLoad(script);
			}
		}, DEFAULT_WRITE_RETRY);
	}

	@Override
	public String lpop(final String key) {
		return callable(new JedisPorxyCallback<String>("lpop", key, true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.lpop(key);
			}
		});
	}

	@Override
	public byte[] lpop(final byte[] key) {
		return callable(new JedisPorxyCallback<byte[]>("lpop",
				CodecHandler.toStr(key), true) {
			@Override
			public byte[] call(Jedis jedis) {
				return jedis.lpop(key);
			}
		});
	}

	@Override
	public Long linsert(final String key, final LIST_POSITION where,
			final byte[] pivot, final byte[] value) {
		byte[] bkey = CodecHandler.encode(key);
		return linsert(bkey, where, pivot, value);
	}

	@Override
	public Long linsert(final String key, final LIST_POSITION where,
			final String pivot, final String value) {
		byte[] bvalue = CodecHandler.encode(value);
		byte[] bpivot = CodecHandler.encode(pivot);

		return linsert(key, where, bpivot, bvalue);
	}

	@Override
	public Long linsert(final byte[] key, final LIST_POSITION where,
			final byte[] pivot, final byte[] value) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("linsert",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.linsert(key, where, pivot, value);
			}
		});
	}

	@Override
	public Boolean rename(final String oldkey, final String newkey) {
		byte[] boldkey = CodecHandler.encode(oldkey);
		byte[] bnewkey = CodecHandler.encode(newkey);

		return rename(boldkey, bnewkey);
	}

	@Override
	public Boolean rename(final byte[] oldkey, final byte[] newkey) {
		if (readOnly) {
			return false;
		}

		return callable(new JedisPorxyCallback<Boolean>("rename",
				CodecHandler.toStr(oldkey), true) {
			@Override
			public Boolean call(Jedis jedis) {
				String result = jedis.rename(oldkey, newkey);

				return REDIS_RET_OK.equalsIgnoreCase(result);
			}
		});
	}

	@Override
	public Long sadd(final String key, final String... members) {
		if (readOnly) {
			return 0l;
		}

		return callable(new JedisPorxyCallback<Long>("sadd", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.sadd(key, members);
			}
		});
	}

	@Override
	public Long srem(final String key, final String... members) {
		if (readOnly) {
			return 0l;
		}

		return callable(new JedisPorxyCallback<Long>("srem", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.srem(key, members);
			}
		});
	}

	@Override
	public Boolean sismember(final String key, final String member) {
		return callable(new JedisPorxyCallback<Boolean>("sismember", key, false) {
			@Override
			public Boolean call(Jedis jedis) {
				return jedis.sismember(key, member);
			}
		});
	}

	@Override
	public Long pfadd(final String key, final String... elements) {
		if (readOnly) {
			return readOnlyDefaultLongValue;
		}
		return callable(new JedisPorxyCallback<Long>("pfadd", key, true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.pfadd(key, elements);
			}
		});
	}

	@Override
	public Long pfcount(final String... keys) {
		return callable(new JedisPorxyCallback<Long>("pfcount",
				Arrays.toString(keys), false) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.pfcount(keys);
			}
		});
	}

	@Override
	public boolean pfmerge(final String destkey, final String... sourcekeys) {
		if (readOnly) {
			return readOnlyDefaultBooleanValue;
		}
		return callable(new JedisPorxyCallback<Boolean>("pfmerge", destkey
				+ "_" + Arrays.toString(sourcekeys), true) {
			@Override
			public Boolean call(Jedis jedis) {
				return REDIS_RET_OK.equalsIgnoreCase(jedis.pfmerge(destkey,
						sourcekeys));
			}
		});
	}

	public <K> K callable(JedisPorxyCallback<K> callback) {
		return callable(callback, max_try_time);
	}

	public <K> K callable(JedisPorxyCallback<K> callback, int tryTime) {
		return callable(callback, tryTime, false);
	}

	public <K> K callable(JedisPorxyCallback<K> callback, int tryTime,
			boolean isMulti) {
		K value = null;
		Jedis jedis = null;
		long start = System.currentTimeMillis();
		long cost = -1;
		int count = 0;
		try {
			while (count++ < tryTime) {
				try {
					jedis = conn.getResource();
					value = callback.call(jedis);
					cost = System.currentTimeMillis() - start;
					if (log.isDebugEnabled() && cost < REDIS_SLOW_TIME) {
						log.debug(getClientSign(jedis) + " "
								+ callback.getName() + ", key: "
								+ callback.getKey() + " result:" + value);
					} else if (cost >= REDIS_SLOW_TIME) {
						log.warn(getClientSign(jedis) + " "
								+ callback.getName() + ", cost " + cost
								+ " key: " + callback.getKey() + " result:"
								+ ((String) value).substring(500));
					}
					conn.returnResource(jedis);
					break;
				} catch (JedisConnectionException jce) {
					conn.returnBrokenResource(jedis);
					log.error(getClientSign(jedis) + " " + callback.getName()
							+ " fail:" + jce);
				} catch (JedisException je) {
					log.error(getClientSign(jedis) + " " + callback.getName()
							+ " fail:" + je);

					if (isSpecialDataException(je)) {

						conn.returnResource(jedis);
					} else {
						conn.returnBrokenResource(jedis);
					}

					if (je.getCause() instanceof SocketTimeoutException) {
						continue;
					}
					if (throwJedisException) {
						throw je;
					}
					break;
				} catch (final Exception e) {
					log.error(getClientSign(jedis) + " " + callback.getName()
							+ " error:", e);
					conn.returnBrokenResource(jedis);
					break;
				}
			}
		} finally {

		}
		return value;
	}

	/**
	 * 某些异常是由于server回来的数据异常，这种场景下，server端的连接还可用，如果盲目close连接，会导致server被摘�?
	 * 从�?导致其他问题
	 * 
	 * @throws Exception
	 */
	public static boolean isSpecialDataException(Exception e) {
		if (!(e instanceof JedisDataException)) {
			return false;
		}

		if (e.getMessage().equals(
				"NOSCRIPT No matching script. Please use EVAL.")) {
			return true;
		}

		return false;
	}

	public void destroy() throws Exception {
		this.close();
	}

	protected String getClientSign(Jedis jedis) {
		try {
			if (jedis != null && jedis.getClient() != null) {
				return "REDIS (" + jedis.getClient().getHost() + ")" + server;
			} else {
				return toString();
			}
		} catch (Exception e) {
			return "REDIS " + server;
		}
	}

	public int getPort() {
		return port;
	}

	@Override
	public Long sadd(final byte[] key, final byte[]... member) {
		if (readOnly) {
			return 0l;
		}
		return callable(new JedisPorxyCallback<Long>("sadd",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.sadd(key, member);
			}
		});
	}

	@Override
	public Set<byte[]> smember(final byte[] key) {
		return callable(new JedisPorxyCallback<Set<byte[]>>("smembers",
				CodecHandler.toStr(key), false) {
			@Override
			public Set<byte[]> call(Jedis jedis) {
				return jedis.smembers(key);
			}
		});
	}

	@Override
	public Long srem(final byte[] key, final byte[]... member) {
		if (readOnly) {
			return 0l;
		}
		return callable(new JedisPorxyCallback<Long>("srem",
				CodecHandler.toStr(key), true) {
			@Override
			public Long call(Jedis jedis) {
				return jedis.srem(key, member);
			}
		});
	}

	@Override
	public Boolean setbit(final String key, final long offset,
			final boolean value) {
		return callable(new JedisPorxyCallback<Boolean>("setbit", key, true) {
			@Override
			public Boolean call(Jedis jedis) {
				return jedis.setbit(key, offset, value);
			}
		});
	}

	@Override
	public Set<String> keys(final String pattern) {
		return callable(new JedisPorxyCallback<Set<String>>("keys", null, false) {
			@Override
			public Set<String> call(Jedis jedis) {
				return jedis.keys(pattern);
			}
		});
	}

	@Override
	public String getset(final String key, final String newValue) {
		return callable(new JedisPorxyCallback<String>("getset", key, true) {
			@Override
			public String call(Jedis jedis) {
				return jedis.getSet(key, newValue);
			}
		});
	}

	@Override
	public byte[] getset(final byte[] key, final byte[] newValue) {
		return callable(new JedisPorxyCallback<byte[]>("getset",
				CodecHandler.toStr(key), true) {
			@Override
			public byte[] call(Jedis jedis) {
				return jedis.getSet(key, newValue);
			}
		});
	}
}
