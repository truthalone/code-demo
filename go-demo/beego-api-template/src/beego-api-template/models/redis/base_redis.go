package redis

import (
	"time"

	"github.com/astaxie/beego"
	"github.com/garyburd/redigo/redis"
)

var (
	db_addr     string = ""
	db_password string = ""

	Session_live_time int = 3600 //session过期时间 单位秒

	redisPool *redis.Pool //redis连接池
)

func init() {
	db_addr = beego.AppConfig.String("redis_addr")
	db_password = beego.AppConfig.String("redis_password")

	var err error
	Session_live_time, err = beego.AppConfig.Int("session_live_time")
	if err != nil {
		Session_live_time = 3600
	}
}

func RedisConnect(dbNum int) redis.Conn {
	conn, _ := redis.Dial("tcp", db_addr, redis.DialPassword(db_password), redis.DialDatabase(dbNum))
	return conn
}

//获取Redis连接池
func newRedisPool(server, password string) (*redis.Pool, error) {
	var err error
	return &redis.Pool{
		MaxIdle:     32,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			var c redis.Conn
			c, err = redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if password != "" {
				if _, err = c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}, err
}

/*
获取redis数据库连接
*/
func GetRedisConnection() (redis.Conn, error) {
	if redisPool == nil {
		var err error
		redisPool, err = newRedisPool(db_addr, db_password)
		if err != nil {
			return nil, err
		}
	}

	return redisPool.Get(), nil
}
