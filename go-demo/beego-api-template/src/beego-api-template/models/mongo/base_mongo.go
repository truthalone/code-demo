package mongo

import (
	"beego-api-template/g"
	"errors"
	"time"

	"github.com/astaxie/beego"
	"gopkg.in/mgo.v2"
)

// 连接mongodb数据库
var (
	MongodbAddr   string = "" //mongodb数据库地址
	MongodbName   string = "" //mongodb数据名称
	MongodbUser   string = "" //mongodb用户名
	MongodbPasswd string = "" //mongodb密码
)

var (
	mongosession *mgo.Session
)

func init() {
	MongodbAddr = beego.AppConfig.String("mongodb_addr")
	MongodbName = beego.AppConfig.String("mongodb_name")
	MongodbUser = beego.AppConfig.String("mongodb_username")
	MongodbPasswd = beego.AppConfig.String("mongodb_passwd")
}

func GetMongoSession() *mgo.Session {
	if mongosession == nil {
		var err error

		if MongodbUser == "" || MongodbPasswd == "" {
			mongosession, err = mgo.Dial(MongodbAddr)
		} else {
			dialInfo := &mgo.DialInfo{
				Addrs:     []string{MongodbAddr},
				Direct:    false,
				Timeout:   time.Second * 30,
				Database:  MongodbName,
				Source:    "admin",
				Username:  MongodbUser,
				Password:  MongodbPasswd,
				PoolLimit: 4096, // Session.SetPoolLimit
			}

			mongosession, err = mgo.DialWithInfo(dialInfo)
		}

		if err != nil {
			g.Log.Error("连接mongodb失败:%s", err.Error())
			return nil
		}
	}

	return mongosession.Clone()
}

func WithMongoCollection(collectionName string, s func(*mgo.Collection) error) error {
	session := GetMongoSession()
	if session == nil {
		return errors.New("获取mongodb连接失败")
	}
	defer session.Close()

	c := session.DB(MongodbName).C(collectionName)
	return s(c)
}
