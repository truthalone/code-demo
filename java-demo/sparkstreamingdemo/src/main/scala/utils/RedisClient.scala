package utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * @author ming
  * @date 2019/12/20 14:45
  */
object RedisClient extends Serializable {
  @transient private var pool: JedisPool = null

  def makePool(host: String, port: Int, password: String): Unit = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(8)
    poolConfig.setMaxIdle(1)
    poolConfig.setMinIdle(0)
    poolConfig.setMaxWaitMillis(10000)

    makePool(poolConfig, host, port, 2000, password)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, password: String): Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle,
      true, false, 10000, password)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long, password: String): Unit = {
    val poolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(maxTotal)
    poolConfig.setMaxIdle(maxIdle)
    poolConfig.setMinIdle(minIdle)
    poolConfig.setTestOnBorrow(testOnBorrow)
    poolConfig.setTestOnReturn(testOnReturn)
    poolConfig.setMaxWaitMillis(maxWaitMillis)

    makePool(poolConfig, redisHost, redisPort, redisTimeout, password)
  }


  def makePool(config: JedisPoolConfig, host: String, port: Int, timeOut: Int, password: String): Unit = {
    if (pool == null) {
      pool = new JedisPool(config, host, port, timeOut, password)
      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }


  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }
}