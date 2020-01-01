package utils

import (
	"crypto/md5"
	"encoding/hex"
	"os"
)

/**
将字符串数组转换成以逗号分隔的字符串
*/
func ConvertToFlat(data []string) string {
	length := len(data)
	if length == 0 {
		return ""
	}

	result := ""
	for k, v := range data {
		result += v
		if k < (length - 1) {
			result += ","
		}
	}

	return result
}

/**
判断文件路径是否存在
*/
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	return os.IsExist(err), nil
}

/**
计算 MD5编码
*/
func GetMd5(data string) string {
	hash := md5.New()
	hash.Write([]byte(data))
	return hex.EncodeToString(hash.Sum(nil))
}
