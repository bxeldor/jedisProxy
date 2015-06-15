package redis.clients.jedisProxy.tests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.tests.commands.JedisCommandTestBase;
import redis.clients.util.SafeEncoder;

public class JedisTest {
  @Test
  public void useWithoutConnecting() {

	String ip;
	try {
		ip = InetAddress.getLocalHost().getHostAddress();
		System.out.println(ip);
	} catch (UnknownHostException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
    Jedis jedis = new Jedis("10.73.11.21",6385);
    for(int i=0;i<1000000;i++){
    	jedis.set(i+"zzzzzzzzz", i+"zzzzzz");
    }
    System.out.println(jedis.get("bx"));
  }
/*
  @Test
  public void checkBinaryData() {
    byte[] bigdata = new byte[1777];
    for (int b = 0; b < bigdata.length; b++) {
      bigdata[b] = (byte) ((byte) b % 255);
    }
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("data", SafeEncoder.encode(bigdata));

    String status = jedis.hmset("foo", hash);
    assertEquals("OK", status);
    assertEquals(hash, jedis.hgetAll("foo"));
  }

  @Test
  public void connectWithShardInfo() {
    JedisShardInfo shardInfo = new JedisShardInfo("localhost", Protocol.DEFAULT_PORT);
    shardInfo.setPassword("foobared");
    Jedis jedis = new Jedis(shardInfo);
    jedis.get("foo");
  }

  @Test(expected = JedisConnectionException.class)
  public void timeoutConnection() throws Exception {
    jedis = new Jedis("localhost", 6379, 15000);
    jedis.auth("foobared");
    jedis.configSet("timeout", "1");
    Thread.sleep(2000);
    jedis.hmget("foobar", "foo");
  }

  @Test(expected = JedisConnectionException.class)
  public void timeoutConnectionWithURI() throws Exception {
    jedis = new Jedis(new URI("redis://:foobared@localhost:6380/2"), 15000);
    jedis.configSet("timeout", "1");
    Thread.sleep(2000);
    jedis.hmget("foobar", "foo");
  }

  @Test(expected = JedisDataException.class)
  public void failWhenSendingNullValues() {
    jedis.set("foo", null);
  }

  @Test(expected = InvalidURIException.class)
  public void shouldThrowInvalidURIExceptionForInvalidURI() throws URISyntaxException {
    Jedis j = new Jedis(new URI("localhost:6380"));
    j.ping();
  }

  @Test
  public void shouldReconnectToSameDB() throws IOException {
    jedis.select(1);
    jedis.set("foo", "bar");
    jedis.getClient().getSocket().shutdownInput();
    jedis.getClient().getSocket().shutdownOutput();
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void startWithUrlString() {
    Jedis j = new Jedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    Jedis jedis = new Jedis("redis://:foobared@localhost:6380/2");
    assertEquals("PONG", jedis.ping());
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void startWithUrl() throws URISyntaxException {
    Jedis j = new Jedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    Jedis jedis = new Jedis(new URI("redis://:foobared@localhost:6380/2"));
    assertEquals("PONG", jedis.ping());
    assertEquals("bar", jedis.get("foo"));
  }

  @Test
  public void shouldNotUpdateDbIndexIfSelectFails() throws URISyntaxException {
    int currentDb = jedis.getDB();
    try {
      int invalidDb = -1;
      jedis.select(invalidDb);

      fail("Should throw an exception if tried to select invalid db");
    } catch (JedisException e) {
      assertEquals(currentDb, jedis.getDB());
    }
  }

  @Test
  public void allowUrlWithNoDBAndNoPassword() {
    Jedis jedis = new Jedis("redis://localhost:6380");
    jedis.auth("foobared");
    assertEquals(jedis.getClient().getHost(), "localhost");
    assertEquals(jedis.getClient().getPort(), 6380);
    assertEquals(jedis.getDB(), 0);

    jedis = new Jedis("redis://localhost:6380/");
    jedis.auth("foobared");
    assertEquals(jedis.getClient().getHost(), "localhost");
    assertEquals(jedis.getClient().getPort(), 6380);
    assertEquals(jedis.getDB(), 0);
  }

  @Test
  public void checkCloseable() {
    jedis.close();
    BinaryJedis bj = new BinaryJedis("localhost");
    bj.connect();
    bj.close();
  }

  @Test
  public void checkDisconnectOnQuit() {
    jedis.quit();
    assertFalse(jedis.getClient().isConnected());
  }
*/
}