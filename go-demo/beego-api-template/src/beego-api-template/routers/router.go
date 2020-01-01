package routers

import (
	"beego-api-template/controllers"
	"github.com/astaxie/beego"
)

func init() {
	beego.Router("/template/v1/test/:param", &controllers.TestController{}, "get:Test")
}
