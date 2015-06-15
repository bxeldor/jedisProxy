package redis.clients.jedisProxy.tests;

import static org.junit.Assert.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import cn.sina.dsp.jedisproxy.CodecHandler;
import cn.sina.dsp.jedisproxy.JedisProxy;

public class JedisProxyTest {
    public static JedisPoolConfig config = new JedisPoolConfig();
	static org.apache.log4j.Logger log= org.apache.log4j.Logger.getLogger("jedisProxy");
	private String zKey = "zset";
	private String zValue = "mem1";
	
	public static JedisProxy jedis;
	@Before
	public void setUp() throws Exception {
		
		config.setMaxTotal(5);
	    config.setMaxIdle(5);
	    config.setMinIdle(3);
	    jedis= new JedisProxy(config,"liudong_cluster","liudong_test","10.73.11.21",6007);
	      }
	
	@Test
	public void testSet() {		
		byte key[]="testSet".getBytes();
		byte value[]="testValue".getBytes();
		System.out.println(jedis.set(key, value));
		String result=jedis.get(CodecHandler.toStr(key));
		assertEquals("testValue", result);		
		byte[] byteResult = jedis.get(key);
		assertEquals(CodecHandler.toStr(byteResult), "testValue");
	}	
	
	@Test
	public void testSet2() {
		String key="testSet";
		long value=-1024;
		System.out.println(jedis.set(key, value));
		String result=jedis.get(key);
		assertEquals(-1024, Long.parseLong(result));
	}	
	
	@Test
	public void testExists() {
		String key="testexists";
		long value=-1024;
		System.out.println(jedis.set(key, value));
		Boolean result=jedis.exists(key);
		assertTrue(result);
	}	
	@Test
	public void del() {
		String key="test";
		long value=-1024;
		System.out.println(jedis.set(key, value));
		long result=jedis.del(new String[]{key,"noExist"});
		assertEquals(1, result);
		String ret=jedis.get(key);
		assertNull(ret);
	}	
	
	@Test
	public void hdelAll() {
		String key="test";
		long value=-1024;
		assertTrue(jedis.set(key, value));
		assertTrue(jedis.hdelAll(key));
		String ret=jedis.get(key);
		assertNull(ret);
	}	
	
	@Test
	public void mget() {
		String key1="testmget",key2="test2",key3="test3";
		String value1 = "1", value2 = "2", value3 = "3";
		
		jedis.set(key1, value1);
		jedis.set(key2, value2);
		jedis.set(key3, value3);
		List<String> result=jedis.mget(key1,key2,key3,"noExist");
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
		assertEquals(value3, result.get(2));
		assertEquals(null, result.get(3));
		
		List<String> keys = new ArrayList<String>();
		keys.add(key1);
		keys.add(key2);
		keys.add(key3);
		keys.add("noExist");
		
		result=jedis.mget(keys);
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
		assertEquals(value3, result.get(2));
		assertEquals(null, result.get(3));
		
		byte[] key1Bytes = CodecHandler.encode(key1);
		byte[] key2Bytes = CodecHandler.encode(key2);
		byte[] key3Bytes = CodecHandler.encode(key3);
		
		List<byte[]> byteResult=jedis.mget(key1Bytes, key2Bytes, key3Bytes);
		assertEquals(value1, CodecHandler.toStr(byteResult.get(0)));
		assertEquals(value2, CodecHandler.toStr(byteResult.get(1)));
		assertEquals(value3, CodecHandler.toStr(byteResult.get(2)));
	}	
	
	@Test
	public void mset() {
		String key1="testmset",key2="test2",key3="test3";
		String value1="1",value2="2",value3="3";
		Boolean result1=jedis.mset(key1,value1,key2,value2,key3,value3);
		System.out.println(result1);
		List<String> result=jedis.mget(key1,key2,key3,"noExist");
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
		assertEquals(value3, result.get(2));
	
		jedis.del(key1, key2, key3);
		
		result=jedis.mget(key1,key2,key3);
		assertEquals(null, result.get(0));
		assertEquals(null, result.get(1));
		assertEquals(null, result.get(2));
		
		Map<byte[], byte[]> maps = new HashMap<byte[], byte[]> ();
		maps.put(CodecHandler.encode(key1), CodecHandler.encode(value1));
		maps.put(CodecHandler.encode(key2), CodecHandler.encode(value2));

		result1=jedis.mset(maps);
		
		result=jedis.mget(key1,key2);
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
	}
	
	@Test
	public void decrBy() {
		String key1="test1";
		jedis.set(key1, 5);
		jedis.decrBy(key1,2);
		String result=jedis.get(key1);
		assertEquals(3,Long.parseLong(result));
	}	
	@Test
	public void decr() {
		String key1="testdecr";
		jedis.set(key1, 5);
		jedis.decr(key1);
		String result=jedis.get(key1);
		assertEquals(4,Long.parseLong(result));
		
		jedis.decr(CodecHandler.encode(key1));
		result=jedis.get(key1);
		assertEquals(3,Long.parseLong(result));

	}
	@Test
	public void incrBy() {
		String key1="testincrBy";
		jedis.set(key1, 5);
		jedis.incrBy(key1,2);
		String result=jedis.get(key1);
		assertEquals(7,Long.parseLong(result));
	}	
	@Test
	public void incr() {
		String key1="testincr";
		jedis.set(key1, 5);
		jedis.incr(key1);
		String result=jedis.get(key1);
		assertEquals(6,Long.parseLong(result));
		
		jedis.incr(CodecHandler.encode(key1));

		result=jedis.get(key1);
		assertEquals(7,Long.parseLong(result));
	}
	
	@Test
	public void hset() {
		String key="testhset",field="fa",value="va";
		jedis.hset(key, field, value);
		String result=jedis.hget(key, field);
		assertEquals(value, result);
		
		byte[] byteResult=jedis.hget(CodecHandler.encode(key), CodecHandler.encode(field));
		assertEquals(value, CodecHandler.toStr(byteResult));
	}	
	
	@Test
	public void hmget() {
		String key="testhmget";	
		String field1="fa",value1="v1";
		String field2="fb",value2="vb";
		Map<byte[],byte[]> keyValueMap=new HashMap<byte[],byte[]>();		
		keyValueMap.put(field1.getBytes(), value1.getBytes());
		keyValueMap.put(field2.getBytes(), value2.getBytes());
		jedis.hmset(key, keyValueMap);
		List<String> result=jedis.hmget(key, field1,field2);
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
		
		List<String> fields = new ArrayList<String>();
		fields.add(field1);
		fields.add(field2);
		result=jedis.hmget(key, fields);
		assertEquals(value1, result.get(0));
		assertEquals(value2, result.get(1));
		
		List<byte[]> byteResult=jedis.hmget(CodecHandler.encode(key), CodecHandler.encode(field1),CodecHandler.encode(field2));
		assertEquals(value1, CodecHandler.toStr(byteResult.get(0)));
		assertEquals(value2, CodecHandler.toStr(byteResult.get(1)));
	}
	
	@Test
	public void hmgetPipeline() {
		String baseKey="testhmget_", baseField1 = "field1", baseField2 = "field2", baseValue1 = "value1", baseValue2="value2";
		
		List<List<String>> list = new ArrayList<List<String>>();
		List<String> values = new ArrayList<String>();
		int size = 10;
		
		for (int i = 0; i < size; i++) {
			String key = baseKey + i;
			String field1 = baseField1 + i, value1 = baseValue1 + i;
			String field2 = baseField2 + i, value2 = baseValue2 + i;
			Map<byte[], byte[]> keyValueMap = new HashMap<byte[], byte[]>();
			keyValueMap.put(field1.getBytes(), value1.getBytes());
			keyValueMap.put(field2.getBytes(), value2.getBytes());
			jedis.hmset(key, keyValueMap);
			
			List<String> pair1 = new ArrayList<String>();
			pair1.add(key);
			pair1.add(field1);
			list.add(pair1);
			
			List<String> pair2 = new ArrayList<String>();
			pair2.add(key);
			pair2.add(field2);
			list.add(pair2);
			
			values.add(value1);
			values.add(value2);
		}	
		
		
		List<String> result = jedis.hmgetPipeline(list);
		
		assertEquals(result.size(), values.size());
		
		for (int i = 0; i < values.size(); i ++) {
			assertEquals(values.get(i), result.get(i));
			assertEquals(values.get(i), result.get(i));
		}
		
		result = jedis.hmget(list);
		
		assertEquals(result.size(), values.size());
		
		for (int i = 0; i < values.size(); i ++) {
			assertEquals(values.get(i), result.get(i));
			assertEquals(values.get(i), result.get(i));
		}
	}
	
	@Test
	public void hincrBy() {
		String key="testhincrBy",field="f1";
		Long value=1L;
		jedis.hset(key, field, value);
		jedis.hincrBy(key, field, 2);
		String result=jedis.hget(key, field);
		assertEquals(3, Long.parseLong(result));
		
		jedis.hincrBy(CodecHandler.encode(key), CodecHandler.encode(field), 2);
		result=jedis.hget(key, field);
		assertEquals(5, Long.parseLong(result));
	}

	@Test
	public void hdel() {
		String key="testhdel",field="f1", value="123";
		jedis.hset(key, field, "123");
		String result=jedis.hget(key,field);
		assertEquals(result, value);
		jedis.hdel(key, field);
		result=jedis.hget(key,field);
		assertNull(result);
	}
	
	@Test
	public void hlen() {
		String key="testhlen";	
		String field1="fa",value1="v1";
		String field2="fb",value2="vb";
		Map<byte[],byte[]> keyValueMap=new HashMap<byte[],byte[]>();		
		keyValueMap.put(field1.getBytes(), value1.getBytes());
		keyValueMap.put(field2.getBytes(), value2.getBytes());
		jedis.hmset(key, keyValueMap);
		Long result=jedis.hlen(key);
		assertEquals(2, result.longValue());
		
		result=jedis.hlen(CodecHandler.encode(key));
		assertEquals(2, result.longValue());
	}	
	
	@Test
	public void hkeys() {
		String key="testkeys";	
		String field1="fa",value1="v1";
		String field2="fb",value2="vb";
		Map<byte[],byte[]> keyValueMap=new HashMap<byte[],byte[]>();		
		keyValueMap.put(field1.getBytes(), value1.getBytes());
		keyValueMap.put(field2.getBytes(), value2.getBytes());
		jedis.hmset(key, keyValueMap);
		Set<String> result=jedis.hkeys(key);
		assertTrue(result.contains(field1));
		assertTrue(result.contains(field2));
		
		Set<byte[]> byteResult=jedis.hkeys(CodecHandler.encode(key));
		
		result.clear();
		for (byte[] bytes : byteResult) {
			result.add(CodecHandler.toStr(bytes));
		}
		
		assertTrue(result.contains(field1));
		assertTrue(result.contains(field2));
	}
	
	@Test
	public void hvals() {
		String key="testhvals";	
		String field1="fa",value1="v1";
		String field2="fb",value2="vb";
		Map<byte[],byte[]> keyValueMap=new HashMap<byte[],byte[]>();		
		keyValueMap.put(field1.getBytes(), value1.getBytes());
		keyValueMap.put(field2.getBytes(), value2.getBytes());
		jedis.hmset(key, keyValueMap);
		List<String> result=jedis.hvals(key);
		assertTrue(result.contains(value1));
		assertTrue(result.contains(value2));
		
		List<byte[]> byteResult=jedis.hvals(CodecHandler.encode(key));
		
		result.clear();
		for (byte[] bytes : byteResult) {
			result.add(CodecHandler.toStr(bytes));
		}
		
		assertTrue(result.contains(value1));
		assertTrue(result.contains(value2));
	}
	
	@Test
	public void hgetAll() {
		String key="testhgetAll";	
		String field1="fa",value1="v1";
		String field2="fb",value2="vb";
		Map<byte[],byte[]> keyValueMap=new HashMap<byte[],byte[]>();		
		keyValueMap.put(field1.getBytes(), value1.getBytes());
		keyValueMap.put(field2.getBytes(), value2.getBytes());
		jedis.hmset(key, keyValueMap);
		Map<String,String> result=jedis.hgetAll(key);
		assertEquals(value1,result.get(field1));
		assertEquals(value2,result.get(field2));
		
		
		Map<byte[],byte[]> byteResult=jedis.hgetAll(CodecHandler.encode(key));
		result.clear();
		
		for (Map.Entry<byte[],byte[]> bytes : byteResult.entrySet()) {
			result.put(CodecHandler.toStr(bytes.getKey()), CodecHandler.toStr(bytes.getValue()));
		}
		
		assertEquals(value1,result.get(field1));
		assertEquals(value2,result.get(field2));
	}
	
	@Test
	public void rpush() {
		String key="testrpush";
		jedis.del(key);
		jedis.rpush(key, "1");
		jedis.rpush(key, "2");
		jedis.rpush(key, 3);

		List<String> result=jedis.lrange(key, 0, 2);
		assertEquals(3, result.size());
		assertEquals("1", result.get(0));
		assertEquals("2", result.get(1));
		assertEquals("3", result.get(2));
	}

	@Test
	public void lpush() {
		String key="testlpush";
		jedis.del(key);
		jedis.lpush(key, "1");
		jedis.lpush(key, "2");
		jedis.lpush(key, 3);
		List<String> result=jedis.lrange(key, 0, 2);
		assertEquals(3, result.size());
		assertEquals("3", result.get(0));
		assertEquals("2", result.get(1));
		assertEquals("1", result.get(2));
	}
	
	@Test
	public void llen() {
		String key="testllen";	
		jedis.lpush(key, "1");
		Long result1=jedis.llen(key);
		jedis.lpush(key, "2");
		Long result2=jedis.llen(key);	
		assertEquals(result1+1, result2.longValue());
	}	
	
	
	@Test
	public void ltrim() {
		String key="testltrim";
		jedis.del(key);
		jedis.lpush(key, "1");
		jedis.lpush(key, "2");
		jedis.lpush(key, "3");
		jedis.ltrim(key, -2, -1);
		List<String> result=jedis.lrange(key, 0, -1);
		assertEquals(2, result.size());
		assertEquals("2", result.get(0));
		assertEquals("1", result.get(1));
		
		jedis.rpush(key, "3");
		List<byte[]> byteResult=jedis.lrange(CodecHandler.encode(key), 0, -1);
		assertEquals(3, byteResult.size());

		jedis.ltrim(CodecHandler.encode(key), -2, -1);
		byteResult=jedis.lrange(CodecHandler.encode(key), 0, -1);
		assertEquals(2, byteResult.size());
		assertEquals("1", CodecHandler.toStr(byteResult.get(0)));
		assertEquals("3", CodecHandler.toStr(byteResult.get(1)));

	}
		
	@Test
	public void lindex() {
		String key="testlindex";
		jedis.lpush(key, "1");
		jedis.lpush(key, "2");
		jedis.lpush(key, "3");
		String result=jedis.lindex(key, 0);
		assertEquals("3", result);
		
		result=CodecHandler.toStr(jedis.lindex(CodecHandler.encode(key), 0));
		assertEquals("3", result);
	}
	
	@Test
	public void lset() {
		String key="testlset";
		jedis.lpush(key, "1");
		jedis.lpush(key, "2");
		jedis.lpush(key, "3");
		String result=jedis.lindex(key, 1);
		assertEquals("2", result);
		jedis.lset(key, 1, "5");
		result=jedis.lindex(key, 1);
		assertEquals("5", result);
		
		jedis.lset(key, 1, 6);
		result=jedis.lindex(key, 1);
		assertEquals("6", result);
		
	}
	
	@Test
	public void lrem() {
		String key="testlrem";
		jedis.del(key);
		jedis.lpush(key, "1");
		jedis.lpush(key, "2");
		jedis.lpush(key, "3");
		jedis.lrem(key, 1, "1");
		List<String> result=jedis.lrange(key,0,-1);
		assertFalse(result.contains("1"));
		
		jedis.lpush(key, 4);
		jedis.lpush(key, 4);
		jedis.lrem(key, 1, 4);

		result=jedis.lrange(key,0,-1);
		assertTrue(result.contains("4"));
		
		jedis.lpush(key, 5);
		jedis.lpush(key, 5);
		jedis.lrem(key, 2, 5);
		result=jedis.lrange(key,0,-1);
		assertFalse(result.contains("5"));
	}
	
	//methods not belong to JedisClient
	@Test
	public void hmdelete(){
		String key="testhmdelete";
		jedis.del(key);
		String field1="f1",field2="f2",field3="f3";
		List<String> fields=new ArrayList<String>();
		fields.add(field1);
		fields.add(field2);
		jedis.hset(key, field1, "v1");
		jedis.hset(key, field2, "v2");
		jedis.hset(key, field3, "v3");
		jedis.hmdelete(key, fields);
		Set<String> result=jedis.hkeys(key);
		assertFalse(result.contains(field1));
		assertFalse(result.contains(field2));
	}
	
	@Test
	public void  hmexists(){
		String key="testhmexists";
		jedis.del(key);
		String field1="f1",field2="f2",field3="f3";
		List<String> fields=new ArrayList<String>();
		fields.add(field1);
		fields.add(field2);
		fields.add("nonfield");
		jedis.hset(key, field1, "v1");
		jedis.hset(key, field2, "v2");
		jedis.hset(key, field3, "v3");
		List<Boolean> result=jedis.hmexists(key,fields);
		assertTrue(result.get(0));
		assertTrue(result.get(1));
		assertFalse(result.get(2));
	}
	
	@Test
	public void  hexists(){
		String key="testhmexists";
		jedis.del(key);
		String field1="f1",field2="f2",field3="f3";
		List<String> fields=new ArrayList<String>();
		fields.add(field1);
		fields.add(field2);
		fields.add("nonfield");
		jedis.hset(key, field1, "v1");
		jedis.hset(key, field2, "v2");
		jedis.hset(key, field3, "v3");
		
		assertTrue(jedis.hexists(key, field1));
		
		List<Boolean> result=jedis.hmexists(key,fields);
		assertTrue(result.get(0));
		assertTrue(result.get(1));
		assertFalse(result.get(2));	
	}
	
	@Test
	public void zadd(){
		byte[] zsetKey = mockKeyForZset();
		byte[] zmember = mockMemberForZset();
		int score = 3;
		jedis.zrem(zsetKey, zmember);
		jedis.zadd(zsetKey, score, zmember);
		int scoreInRedis = jedis.zscore(zsetKey, zmember).intValue();
		
		assertEquals(score, scoreInRedis);
		jedis.zrem(zsetKey, zmember);

		jedis.zadd(zKey, score, zValue);
		assertEquals(score, scoreInRedis);
		
	}
	
	@Test
    public void zrem(){
		byte[] zsetKey = mockKeyForZset();
		byte[] member = mockMemberForZset();
		int score = 5;
    	jedis.zadd(zsetKey, score, member);
    	long count1 = jedis.zcard(zsetKey);
    	
    	jedis.zrem(zsetKey, member);
    	long count2 = jedis.zcard(zsetKey);
    	
    	assertEquals(count1, count2 + 1);
    }
	
	@Test
    public void zremrangeByRank(){
		byte[] key = mockKeyForZset();
		byte[] member = mockMemberForZset();
		
		jedis.zadd(key, 1, member);
		
		long count1 = jedis.zcard(key);
		jedis.zremrangeByRank(key, 0, 0);
		
		long count2 = jedis.zcard(key); 
		
		assertEquals(count1, count2 + 1);
	}
	
	@Test
    public void zremrangeByScore(){
		byte[] key = mockKeyForZset();
		byte[] member = mockMemberForZset();
		
		jedis.zadd(key, 1, member);
		long count1 = jedis.zcard(key);
		jedis.zremrangeByScore(key, 1, 1);
		long count2 = jedis.zcard(key);
		
		assertTrue(count1 > count2);
    }
    
	@Test
    public void zincrby(){
		byte[] key = mockKeyForZset();
		byte[] member = mockMemberForZset();
		
		jedis.zadd(key, 1, member);
		double score1 = jedis.zscore(key, member);
		jedis.zincrby(key, 1, member);
		double score2 = jedis.zscore(key, member);
		
		assertEquals(score1 , score2 - 1);
	}
    
	@Test
    public void zrank(){
    	byte[] key = mockKeyForZset();
		byte[] member = mockMemberForZset();
		
		jedis.zadd(key, 1, member);
		long rank = jedis.zrank(key, member);
		assertTrue(rank >= 0);
    }
    
    @Test
    public void zrange(){
    	byte[] key = mockKeyForZset();
		byte[] member = mockMemberForZset();
		int start = 0;
		int end = 1;
		
		jedis.del(key);
		jedis.zrem(key, member);
		Set<byte[]> values1 = jedis.zrange(key, 0, 1);
		jedis.zadd(key, 1, member);
		Set<byte[]> values2 = jedis.zrange(key, start, end);
		assertEquals(values1 == null ? 0 : values1.size(), values2.size() - 1);
    }
      
    @Test
    public void zrangeByScore(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	double min = 1;
    	double max = 1;
    	
    	jedis.zrem(key, member);
    	Set<byte[]> values1 = jedis.zrangeByScore(key, min, max);
    	jedis.zadd(key, 1, member);
    	Set<byte[]> values2 = jedis.zrangeByScore(key, min, max);
    	
    	assertEquals(values1 == null ? 0 : values1.size(), values2.size() - 1);
    }
    
    @Test
    public void zrangeByScoreWithOffsetCount(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	double min = 1;
    	double max = 1;
    	int offset = 0;
    	int count = 1;
    	
    	jedis.zadd(key, 1, member);
    	Set<byte[]> values2 = jedis.zrangeByScore(key, min, max, offset, count);
    	
    	assertEquals(values2.size(), 1);
    }
    
    @Test
    public void zrangeByScoreWithScores(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	double min = 1;
    	double max = 1;
    	
    	jedis.zrem(key, member);
    	Set<Tuple> values1 = jedis.zrangeByScoreWithScores(key, min, max);
    	jedis.zadd(key, 1, member);
    	Set<Tuple> values2 = jedis.zrangeByScoreWithScores(key, min, max);
    	
    	assertEquals(values1 == null ? 0 : values1.size(), values2.size() - 1);
    }
    
    @Test
    public void zrangeByScoreWithScoresWithOffsetCount(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	double min = 1;
    	double max = 1;
    	int offset = 0;
    	int count = 1;
    	
    	jedis.zadd(key, 1, member);
    	Set<Tuple> values1 = jedis.zrangeByScoreWithScores(key, min, max, offset, count);
    	
    	assertEquals(values1.size(), 1);
    }
    
    @Test
    public void zrevrank(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	
    	jedis.zadd(key, 1, member);
    	long rank = jedis.zrank(key, member);
    	long rankRev = jedis.zrevrank(key, member);
    	long cardinality = jedis.zcard(key);
    	
    	assertEquals(rank + rankRev + 1, cardinality);
    	
    }
    
    @Test
    public void zrevrange(){
    	//byte[] key = mockKeyForZset();
    	//byte[] member = mockMemberForZset();
    	
    	//jedis.zadd(key, 1, member);
    	//int cardinality = jedis.zcard(key).intValue();
    	//Set<byte[]> values1 = jedis.zrevrange(key, 0, 1);
    	//Set<byte[]> values2 = jedis.zrevrange(key, cardinality - 1, cardinality - 1);
    	//assertEquals(values1.size(), values2.size());
    }
    
    @Test
    public void zcard(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	
    	jedis.zrem(key, member);
    	int cardinality1 = jedis.zcard(key).intValue();
    	jedis.zadd(key, 1, member);
    	int cardinality2 = jedis.zcard(key).intValue();
    	
    	assertEquals(cardinality1, cardinality2 - 1);
    }
    
    
    @Test
    public void zcount(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	
    	jedis.zadd(key, 1, member);
    	int zcount1 = jedis.zcount(key, 0, 1).intValue();
    	jedis.zadd(key, 5, member);
    	int zcount2 = jedis.zcount(key, 0, 1).intValue();
    	
    	assertEquals(zcount1 - 1, zcount2);
    	
    }
    
    @Test
    public void zscore(){
    	byte[] key = mockKeyForZset();
    	byte[] member = mockMemberForZset();
    	double score = 1.0;
    	
    	jedis.zadd(key, score, member);
   // 	assertEquals(score, jedis.zscore(key, member));	
    }
    
    @Test
    public void zrevrangeWithScores() {
		jedis.del(zKey);
		jedis.zadd(zKey, 1, zValue + "1");		
		jedis.zadd(zKey, 3, zValue + "2");
		jedis.zadd(zKey, 5, zValue + "3");

		Set<Tuple> result = jedis.zrevrangeWithScores(CodecHandler.encode(zKey), 1, 3);

		assertEquals(result.size(), 2);
    }
    
    @Test
    public void hllCommands(){
    	String pfkey1 = "pfkey1";
    	String pfkey2 = "pfkey2";
    	String pfkeyMege = "pfkey_merge";
    	
    	jedis.del(new String[]{pfkey1});
    	
    	jedis.pfadd(pfkey1, new String[]{"1", "2", "3", "1", "a", "2"});
    	assertTrue(4==jedis.pfcount(pfkey1));
    	
    	int count2 = 100;
    	for(int i = 0; i < count2; i++){
    		jedis.pfadd(pfkey2, "k2_" + i);
    	}
    	assertTrue(count2 == jedis.pfcount(pfkey2));
    	
    	boolean mergers = jedis.pfmerge(pfkeyMege, pfkey1, pfkey2);
    	assertTrue(mergers);
    	assertTrue(4 + count2 == jedis.pfcount(pfkeyMege));
    	System.out.println("======= hll success!========");
    }
    
    @Test
    public void getsetCommand() {
    	String key = "test";
    	String value1 = "value1";
    	String value2 = "value2";
    	
    	jedis.del(key);
    	String returnValue1 = jedis.getset(key, value1);
    	assertEquals(null, returnValue1);
    	String returnValue2 = jedis.getset(key, value2);
    	assertEquals(value1, returnValue2);
    	String returnValue3 = jedis.get(key);
    	assertEquals(value2, returnValue3);
    }
    
    @Test
    public void getsetBinaryCommand() {
    	byte[] key = CodecHandler.encode("test");
    	byte[] value1 = CodecHandler.encode("value1");
    	byte[] value2 = CodecHandler.encode("value2");
    	byte[] defaultValue = null;
    	
    	jedis.del(key);
    	byte[] returnValue1 = jedis.getset(key, value1);
    	assertTrue(Arrays.equals(defaultValue, returnValue1));
    	byte[] returnValue2 = jedis.getset(key, value2);
    	assertTrue(Arrays.equals(value1, returnValue2));
    	byte[] returnValue3 = jedis.get(key);
    	assertTrue(Arrays.equals(value2, returnValue3));
    }
    
    @Test
    public void expireCommans() throws InterruptedException{
    	String key = "exp_k1";
    	String value = "val_k1";
    	int ttl = 3;
    	boolean rs;
    	
    	jedis.set(key, value);
    	rs = jedis.expireAt(key, System.currentTimeMillis()/1000 + 3);
    	assertTrue(rs);
    	assertEquals(value, jedis.get(key));
    	Thread.sleep((ttl + 1) * 1000);
    	assertNull(jedis.get(key));
    	
    	jedis.set(key, value);
    	rs = jedis.expire(key, ttl);
    	assertTrue(rs);
    	assertTrue(ttl == jedis.ttl(key));
    	assertEquals(value, jedis.get(key));
    	try {
			Thread.sleep((ttl + 1) * 1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
    	assertNull(jedis.get(key));
    	
    	jedis.set(key, value);
    	rs = jedis.expire(key, ttl);
    	assertTrue(rs);
    	assertTrue(ttl == jedis.ttl(key));
    	rs = jedis.persist(key);
    	assertTrue(rs);
    	try {
			Thread.sleep((ttl + 1) * 1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	assertEquals(value, jedis.get(key));
    	
    	jedis.setex(key, ttl, value);
    	assertTrue(ttl == jedis.ttl(key));
    	assertTrue(value.equals(jedis.get(key)));
    	//ApiUtil.safeSleep(ttl + 1);
    	try {
			Thread.currentThread().sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	assertNull(jedis.get(key));
    	
    	System.out.println("============ expire test successed! ============");
    }

	
    private byte[] mockKeyForZset(){
		return CodecHandler.encode(zKey);
	}
	
	private byte[] mockMemberForZset(){
		return CodecHandler.encode(zValue);
	}
	
	private String getIp(String host) {
		String ip = host;

		for (int i = 0; i < 3; i++) {
			boolean isBroken = false;
			try {
				ip = InetAddress.getByName(host).getHostAddress();
			} catch (Exception e) {
				isBroken = true;
			}

			if (!isBroken) {
				break;
			}

			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}

		return ip;
	}
}