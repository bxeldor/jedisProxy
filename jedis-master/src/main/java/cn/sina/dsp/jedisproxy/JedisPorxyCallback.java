package cn.sina.dsp.jedisproxy;

import redis.clients.jedis.Jedis;

public abstract class JedisPorxyCallback<V> {
	private String name;
	private String key;
	private boolean isWriter;

	public JedisPorxyCallback(String name, String key, boolean isWriter) {
		super();
		this.name = name;
		this.key = key;
		this.isWriter = isWriter;
	}

	public abstract V call(Jedis jedis);
	
	public String getName() {
		return name;
	}


	public String getKey() {
		return key;
	}


	public void setName(String name) {
		this.name = name;
	}


	public void setKey(String key) {
		this.key = key;
	}

	public boolean isWriter() {
		return isWriter;
	}

	public void setWriter(boolean isWriter) {
		this.isWriter = isWriter;
	}

}
