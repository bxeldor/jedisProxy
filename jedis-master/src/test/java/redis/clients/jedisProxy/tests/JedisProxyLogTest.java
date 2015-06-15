package redis.clients.jedisProxy.tests;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import cn.sina.dsp.jedisproxy.JedisProxy;
import com.google.common.collect.ImmutableList;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

public class JedisProxyLogTest {
	HostAndPort hostAndPort = new HostAndPort("10.73.11.21",22223);
	ImmutableList<HostAndPort> of = ImmutableList.of(hostAndPort);
	
	public static JedisPoolConfig config = new JedisPoolConfig();
	static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger("jedis");
	static org.apache.log4j.Logger log2 = org.apache.log4j.Logger.getLogger("jedisProxy");
	
	@Before
	public void setUp() throws Exception {
		config.setMaxTotal(5);
	    config.setMaxIdle(5);
	    config.setMinIdle(3);
	      }
	
	@Test
	public void testSet() {
		
		PropertyConfigurator.configure("log4j.properties");
		log.error("fff");
		log2.info("ssss");
		log2.debug("ddddd");
		log2.error("err");
	}	
}