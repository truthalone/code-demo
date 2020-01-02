package g

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
)

type JsonRet struct {
	Result    bool        `json:"result"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data"`
	DebugInfo string      `json:"-"`
}

const (
	LogConsole  int    = 0
	LogFile     int    = 1
	LogFileName string = "log/app.log"
)

var Log *logs.BeeLogger

func init() {
	Log = logs.NewLogger(5000)

	logtype, err := beego.AppConfig.Int("logtype")
	if err != nil {
		fmt.Println(err.Error())
		logtype = LogConsole
	}

	if logtype == LogFile {
		logfile := beego.AppConfig.String("logfile")
		if len(logfile) == 0 {
			logfile = LogFileName
		}

		dirPath := filepath.Dir(logfile)
		if len(dirPath) > 0 {
			os.MkdirAll(dirPath, 0777)
		}

		Log.SetLogger("file", fmt.Sprintf(`{"filename":"%s"}`, logfile))

	} else {
		Log.SetLogger("console", "")
	}

	Log.SetLevel(logs.LevelDebug)
	Log.EnableFuncCallDepth(true)
}


