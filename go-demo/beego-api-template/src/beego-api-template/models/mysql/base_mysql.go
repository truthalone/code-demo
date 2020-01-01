package mysql

import (
	"github.com/astaxie/beego"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
)

// 连接mysql数据库
func init() {
	dbHost := beego.AppConfig.String("dbhost")
	dbPort := beego.AppConfig.String("dbport")
	dbUser := beego.AppConfig.String("dbuser")
	dbPassword := beego.AppConfig.String("dbpassword")
	dbName := beego.AppConfig.String("dbname")
	if dbPort == "" {
		dbPort = "3306"
	}
	connectStr := dbUser + ":" + dbPassword + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName + "?charset=utf8"
	orm.RegisterDataBase("default", "mysql", connectStr)

	idleConn := beego.AppConfig.DefaultInt("idle_conns", 16)
	maxConn := beego.AppConfig.DefaultInt("max_conns", 128)

	orm.SetMaxIdleConns("default", idleConn)
	orm.SetMaxOpenConns("default", maxConn)
}
