package redis.clients.jedisProxy.tests;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import cn.sina.dsp.jedisproxy.JedisProxy;
import redis.clients.jedis.JedisPoolConfig;

class thread2 implements Runnable{
	private final String threadName;
	private JedisProxy jedisProxy;
	
	public thread2(String threadName,JedisProxy jedisProxy){
		this.threadName = threadName;
		this.jedisProxy = jedisProxy;
	}
	public void run(){
		JedisProxy.log.info(threadName);
		for(int i=0;i<10;i++){
			jedisProxy.set("testForShow","ShowAnswer");
			jedisProxy.get("testForShow");
		}
	}
}
public class JedisProxy4ShowTest {
	public static JedisPoolConfig config = new JedisPoolConfig();
	static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger("jedisProxy");
	
	@Before
	public void setUp() throws Exception {
		config.setMaxTotal(3200);
	    config.setMaxIdle(3200);
	    config.setMinIdle(640);
	      }
	
	@Test
	public void testShow() {
		PropertyConfigurator.configure("log4j.properties");
		JedisProxy jedisProxy = new JedisProxy(config,"liudong_cluster","baix","10.73.11.21",6007);
		for(int i=10;i<100;i++){
			new thread2("线程"+String.valueOf(i),jedisProxy);
		}
		try {
			Thread.sleep(500000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}	
}