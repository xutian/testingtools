package utils

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	kafka "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
)

type Handler interface {
	Do(data *bytes.Buffer)
}

type HttpHandler struct {
	Cli   *http.Client
	Url   string
	Topic string
	Conf  *Config
}

func NewHttpHandler(topic string, conf *Config) *HttpHandler {
	return &HttpHandler{
		Topic: topic,
		Cli:   &http.Client{},
		Url:   fmt.Sprintf("http://%s/dataload?topic=%s", conf.Eip, topic),
		Conf:  conf,
	}
}

func (h *HttpHandler) Do(conf *Config, data *bytes.Buffer, chanOut *chan *Statistician) error {

	reader := bytes.NewReader(data.Bytes())
	request, p_err := http.NewRequest("POST", h.Url, reader)
	if p_err != nil {
		log.Errorf("Packet http request with error, %v", p_err)
		return p_err
	}
	statis := NewStatistician(h.Topic)
	defer request.Body.Close()
	if conf.DataFmt == "avro" {
		request.Header.Add("Context-Type", "avro")
		request.Header.Add("Content-Type", "application/avro")
	} else {
		request.Header.Add("Context-Type", "csv")
		request.Header.Add("Content-Type", "text/csv")
	}

	request.Header.Add("Connection", "keep-alive")
	request.Header.Add("User", h.Conf.DpUser)
	request.Header.Add("Password", h.Conf.DpPasswd)

	request.Header.Add("Transfer-Encoding", "chunked")
	startTime := time.Now()
	response, s_err := h.Cli.Do(request)
	if s_err != nil {
		log.Errorf("Sent http request with error, %v", s_err)
		return s_err
	}
	statis.SentTime = time.Since(startTime).Milliseconds()
	msgBytes := int64(len(data.Bytes()))
	h.Cli.CloseIdleConnections()
	defer response.Body.Close()
	content, err := ioutil.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		statis.State = false
		if err != nil {
			log.Errorln(err)
		}
		log.Errorf("Response code: %v, %s", response.StatusCode, string(content))
	} else {
		statis.State = true
		statis.SentBytes = msgBytes
		log.Debugf("Response code: %v, %s", response.StatusCode, string(content))
	}
	*chanOut <- statis
	return nil
}

type KafkaHandler struct {
	Brokers []string
	Topic   string
	IsAsync bool
	Writer  *kafka.Writer
	Conf    *Config
}

func NewKafkaHandler(topic string, conf *Config) *KafkaHandler {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:    conf.Brokers,
		Topic:      topic,
		Balancer:   &kafka.RoundRobin{},
		BatchBytes: 30 * 1024 * 1024,
		Async:      true,
	})
	handler := KafkaHandler{
		Brokers: conf.Brokers,
		Topic:   topic,
		IsAsync: true,
		Writer:  writer,
		Conf:    conf,
	}
	return &handler
}

func (k *KafkaHandler) Do(conf *Config, data *bytes.Buffer, chanOut *chan *Statistician) {
	defer k.Close()
	dataBytes := data.Bytes()
	msg := kafka.Message{
		Key:   []byte("1"),
		Value: dataBytes,
	}
	statis := NewStatistician(k.Topic)
	startTime := time.Now()
	err := k.Writer.WriteMessages(context.Background(), msg)
	statis.SentTime = time.Since(startTime).Microseconds()
	if err != nil {
		log.Errorf("Sent messgae to kafka with errr, %v", err)
		statis.State = false
	} else {
		statis.State = true
		statis.SentBytes = int64(len(dataBytes))
	}
	*chanOut <- statis
}

func (k *KafkaHandler) Close() {
	k.Writer.Close()
}
