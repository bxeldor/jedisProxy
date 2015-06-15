package cn.sina.dsp.jedisproxy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Tuple;

public interface JedisProxyClient {

	public boolean expire(final String key, final int seconds);
	public boolean expireAt(final String key, final long unixTime);
	public Long ttl(final String key);
	public boolean persist(final String key);
	
    public Boolean set(final byte[] key, final byte[] value);
    public Boolean set(final String key, final byte[] value);
    public Boolean set(final String key, final String value);
    public Boolean set(final String key, final Number value);
    public <T extends Serializable> Boolean set(final String key, final T value);

    public Boolean setex(final String key, final int seconds, final String value);
    
    public byte[] get(final byte[] key);
    public String get(final String key);

    public Boolean exists(final byte[] key);
    public Boolean exists(String key);
    public Long del(final byte[]... keys);
    public Long del(final String... keys);

    public List<byte[]> mget(final byte[]... keys);
    public List<String> mget(final String... keys);
    public Map<String,String> mgetMap(final String... key);

    public Boolean mset(final byte[]... keysvalues);
    public Boolean mset(final String... keysvalues);
    public Boolean mset(final Map<byte[], byte[]> keyValueMap);

    public Long decrBy(final byte[] key, final long integer);
    public Long decrBy(final String key, final long integer);
    
    public Long decr(final byte[] key);
    public Long decr(final String key);
    
    public Long incrBy(final byte[] key, final long integer);
    public Long incrBy(final String key, final long integer);
    
    public Long incr(final byte[] key);
    public Long incr(final String key);

    public Long hset(final byte[] key, final byte[] field, final byte[] value);
    public Long hset(final String key, final String field, final byte[] value);
    public Long hset(final String key, final String field, final String value);
    public Long hset(final String key, final String field, final Number value);
    public <T extends Serializable> Long hset(final String key, final String field, final T value);
    
    public byte[] hget(final byte[] key, final byte[] field);
    public String hget(final String key, final String field);

    public Boolean hmset(final byte[] key, final Map<byte[], byte[]> keyValueMap);
    public Boolean hmset(final String key, final Map<byte[], byte[]> keyValueMap);

    public List<byte[]> hmget(final byte[] key, final byte[]... fields);
    public List<String> hmget(final String key, final String... fields);
    
    
    public Long hincrBy(final byte[] key, final byte[] field, final long value);
    public Long hincrBy(final String key, final String field, final long value);

    public Boolean hdel(final byte[] key, final byte[] field);
    public Boolean hdel(final String key, final String field);
    
    public Long hlen(final byte[] key);
    public Long hlen(final String key);
    
    public Set<byte[]> hkeys(final byte[] key);
    public Set<String> hkeys(final String key);
    
    public List<byte[]> hvals(final byte[] key);
    public List<String> hvals(final String key);
    
    public Map<byte[], byte[]> hgetAll(final byte[] key);
    public Map<String, String> hgetAll(final String key);
    
    public Long rpush(final byte[] key, final byte[] value);
    public Long rpush(final String key, final byte[] value);
    public Long rpush(final String key, final byte[]... value);
    public Long rpush(final byte[] key, final byte[]... value);
    public Long rpush(final String key, final String value);
    public Long rpush(final String key, final String... value);
    public Long rpush(final String key, final Number value);
    public <T extends Serializable> Long rpush(final String key, final T value);
    
    public Long lpush(final byte[] key, final byte[] value);
    public Long lpush(final String key, final byte[] value);
	public Long lpush(final byte[] key, final byte[]... values);
    public Long lpush(final String key, final byte[]... values);
    public Long lpush(final String key, final String value);
	public Long lpush(final String key, final String... values);
    public Long lpush(final String key, final Number value);
    public <T extends Serializable> Long lpush(final String key, final T value);
    
    public Long llen(final byte[] key);
    public Long llen(final String key);
    
    public List<byte[]> lrange(final byte[] key, final int start, final int end);
    public List<String> lrange(final String key, final int start, final int end);
    
    public Boolean ltrim(final byte[] key, final int start, final int end);
    public Boolean ltrim(final String key, final int start, final int end);
    
    public byte[] lindex(final byte[] key, final int index);
    public String lindex(final String key, final int index);
    
    public Boolean lset(final byte[] key, final int index, final byte[] value);
    public Boolean lset(final String key, final int index, final byte[] value);
    public Boolean lset(final String key, final int index, final String value);
    public Boolean lset(final String key, final int index, final Number value);
    public <T extends Serializable> Boolean lset(final String key, final int index, final T value);
    
    public Long lrem(final byte[] key, final int count, final byte[] value);
    public Long lrem(final String key, final int count, final byte[] value);
    public Long lrem(final String key, final int count, final String value);
    public Long lrem(final String key, final int count, final Number value);
    public <T extends Serializable> Long lrem(final String key, final int count, final T value);

    /**
	 * ================================================
	 * methods for sorted set zset
	 * ================================================
	 * */
	public Long zadd(byte[] key, double score, byte[] member);
    public Long zrem(byte[] key, byte[] member);
    public Long zremrangeByRank(byte[] key, int start, int end);
    public Long zremrangeByScore(byte[] key, double start, double end);
    
    public Double zincrby(byte[] key, double score, byte[] member);
    
    public Long zrank(byte[] key, byte[] member);
    
    public Set<byte[]> zrange(byte[] key, int start, int end);
    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end);
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max);
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset,
            int count);
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max);
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max,
            int offset, int count);
    
    public Long zrevrank(byte[] key, byte[] member);
    public Set<byte[]> zrevrange(byte[] key, int start, int end);
    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end);
    
    public Long zcard(byte[] key);
    public Long zcount(byte[] key, double min, double max);
    public Double zscore(byte[] key, byte[] member);
    
    public Object evalsha(final String sha1, int keyCount, final String... keys);
        
    // 还没使用过的接口,谨慎使用  
	//public List<Object> pipeline(JedisPipelineReadCallback callback);
    // 还没使用过的接口,谨慎使用 
	//public List<Object> pipeline(JedisPipelineWriteCallback callback);

	public String flushDB();
	
	public String lpop(final String key);
	public byte[] lpop(final byte[] key);
	
	public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value);

	public Long linsert(final String key, final LIST_POSITION where, final byte[] pivot, final byte[] value);
	
	public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot, final byte[] value);

	public Boolean rename(final String oldkey, final String newkey);
	public Boolean rename(final byte[] oldkey, final byte[] newkey);

    public String scriptLoad(final String script);
    
    /**
     * 该接口只支持写入操作，写入master, 可以支持单个或�?批量操作，由业务方自行控�?
     * 
     * <pre>
     * 	demo:
     * 	
     * 		final String key1 = "hi", value1 = "boy";  
     * 		final String key2 = "hi", value2 = "man";
     * 		JedisPort.callUpdate ( new JedisPortUpdateCallback<List<Boolean>>() {
     * 			
     * 			public List<Boolean> call(Jedis jedis) {
     * 				
     * 				List<Boolean> result = new ArrayList<Boolean>();
     * 	
     * 				result.add(jedis.set(key1,value1);
     * 				result.add(jedis.set(key2,value2);
     * 			
     * 				return result;
     * 			}
     * 		});
     * 
     * 		transation 相关的也可以在这里做 
     * 		
     * </pre>
     * 
     * @return
     */
  //  public <V> V callUpdate(JedisPortUpdateCallback<V> callback);
    
    
	public Long sadd(final String key, final String... members);
	public Long sadd(final byte[] key, final byte[]... member);
	public Set<byte[]> smember(final byte[] key);
	public Long srem(final String key, final String... members);
	public Long srem(final byte[] key, final byte[]... member);
	public Boolean sismember(final String key, final String member);
	
	public Long pfadd(final String key, final String... elements);
	/*
	 * careful: pfcount 在reids原始是一个蹩脚（隐式）的write指令，eredis强制改成了非write指令，待观察效果 fishermen 2014.6.25 \
	 * */
	public Long pfcount(final String... keys);
	public boolean pfmerge(final String destkey, final String... sourcekeys);
	
    public Boolean setbit(final String key, final long offset, final boolean value);
    
    public Set<String> keys(final String pattern);
    
    //增加getset命令封装  tangyang 2014.10.16
    public String getset(final String key, final String newValue);
    public byte[] getset(final byte[] key, final byte[] newValue);
}
