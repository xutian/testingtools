package utils

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type Config struct {
	TotalMessageSize int
	Topics           []string
	MessageSize      int
	Threads          int
	FlowCtrl         bool
	Interval         int
	SchemaId         int
	Brokers          []string
	MethodId         int
	Eip              string
	MessageNum       int
	RunTimeout       float64
	DpUser           string
	DpPasswd         string
	DataFmt          string
}

func NewConfByFile(path string) *Config {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Read conf file %s with error %v", path, err)
	}
	msgSize := viper.GetInt("required.recordnum") // RowNumPerFile
	msgNum := viper.GetInt("required.sndnum")

	config := &Config{
		Topics:           viper.GetStringSlice("required.topics"),
		MessageSize:      msgSize,
		Threads:          viper.GetInt("required.threadsnum"),
		MessageNum:       msgNum, // FileNum
		RunTimeout:       viper.GetFloat64("required.runtostop"),
		FlowCtrl:         viper.GetBool("optional.flow"),
		Interval:         viper.GetInt("optional.flowinterval"),
		SchemaId:         viper.GetInt("required.schemaname"),
		Brokers:          viper.GetStringSlice("required.brokerips"),
		MethodId:         viper.GetInt("test.usemethod"),
		Eip:              viper.GetString("required.eip"),
		TotalMessageSize: msgNum * msgSize,
		DpUser:           viper.GetString("dpconf.user"),
		DpPasswd:         viper.GetString("dpconf.pwd"),
		DataFmt:          viper.GetString("required.datafmt"),
	}
	return config
}

func (c *Config) Validate() {

	if len(c.Topics) < 1 {
		log.Fatalln("缺少必填项：topic, 请修改config")
	}
	if c.RunTimeout > 0 && c.MessageNum > 0 {
		log.Fatalln("总时长和总发送数量sndnum不能同时大于0,请修改config")
	}
	switch c.DataFmt {
		case "avro":
		case "csv":
		default:
		   log.Fatalf("不支持的数据格式%v, 请选择csv或avro", c.DataFmt)
	}
}
