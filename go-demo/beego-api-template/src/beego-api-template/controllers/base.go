package controllers

import (
	"beego-api-template/g"
	"bytes"
	"fmt"
	"github.com/astaxie/beego"
)

type BaseController struct {
	beego.Controller
	NotAutoJson bool
	bJsonp      bool
	result      g.JsonRet
}

const (
	ErrJson = `{"status":false,"message":"","result":null}`
	OkJson  = `{"status":true,"message":"","result":`
)

var (
	runModel = "" //程序运行模式
)

func init() {
	runModel = beego.AppConfig.String("RunMode")
}

func (b *BaseController) Prepare() {
	if b.Ctx.Request.Method == "OPTIONS" {
		b.Ctx.Output.SetStatus(204)
		b.Ctx.Output.Header("Access-Control-Allow-Origin", "*")
		b.Ctx.Output.Header("Access-Control-Allow-Methods", "GET,POST")
		b.Ctx.Output.Header("Access-Control-Allow-Headers",
			"Origin, No-Cache, X-Requested-With, Cache-Control, Content-Type,Authorization,token")

		b.ServeJSON(true)
		b.StopRun()
		return
	}

	callback := b.GetString("callback")
	if callback != "" {
		b.bJsonp = true
	}
}

func (b *BaseController) Finish() {
	w := b.Ctx.ResponseWriter
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Access-Control-Allow-Headers",
		"Origin, No-Cache, X-Requested-With, Cache-Control, Content-Type,Authorization,token")
	if b.NotAutoJson {
		if b.result.Result {
			str := b.result.Data.(string)
			buf := bytes.Buffer{}
			buf.Grow(len(str) + 128)
			buf.WriteString(OkJson)
			buf.WriteString(str)
			buf.WriteString("}")
			b.Ctx.Output.ContentType(".json")
			b.Ctx.WriteString(buf.String())

		} else {
			b.Ctx.Output.ContentType(".json")
			b.Ctx.WriteString(ErrJson)
		}
	} else {
		if b.bJsonp {
			b.Data["jsonp"] = b.result
			b.ServeJSONP()
		} else {
			b.Data["json"] = b.result
			b.ServeJSON()
		}
	}
}

//设置返回错误格式
func (b *BaseController) SetError(err string) {
	b.result.Message = err
	b.result.Result = false
}

//设置成功返回数据格式
func (b *BaseController) SetData(data interface{}) {
	b.NotAutoJson = false
	b.result.Data = data
	b.result.Result = true
}

//设置带有调试信息的错误
//有些错误信息，如数据库操作错误，不应在正式发布后，返回给前端
func (b *BaseController) SetErrorWithDebug(err string, debugInfo string) {
	errInfo := err
	if runModel == "dev" {
		errInfo = fmt.Sprintf("err:%s  debug:%s", err, debugInfo)
	}

	b.result.DebugInfo = debugInfo
	b.result.Message = errInfo
	b.result.Result = false
}
