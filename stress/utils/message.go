package utils

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math/rand"

	"net"
	"reflect"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/avro.v0"
)

var DataSchema = `{
	"type":"record",
    "name":"mpp_bus_pro",
	"fields":[
		{
			"name":"c_netnum",
			"type":"int"
		},
		{
			"name":"c_ip",
			"type":"long"
		},
		{
			"name":"c_flowid",
			"type":"string"
		},
		{
			"name":"c_src_ipv4",
			"type":"long"
		},
		{
			"name":"c_src_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_src_port",
			"type":"int"
		},
		{
			"name":"c_s_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_s_tunnel_port",
			"type":"int"
		},
		{
			"name":"c_dest_ipv4",
			"type":"long"
		},
		{
			"name":"c_dest_ipv6",
			"type":"bytes"
		},
		{
			"name":"c_dest_port",
			"type":"int"
		},
		{
			"name":"c_d_tunnel_ip",
			"type":"long"
		},
		{
			"name":"c_d_tunnel_port",
			"type":"int"
		},
		{
			"name":"c_packet_group",
			"type":"int"
		},
		{
			"name":"c_proto_type",
			"type":"int"
		},
		{
			"name":"c_connect_status",
			"type":"int"
		},
		{
			"name":"c_direct",
			"type":"int"
		},
		{
			"name":"c_server_dir",
			"type":"int"
		},
		{
			"name":"c_up_packets",
			"type":"long"
		},
		{
			"name":"c_up_bytes",
			"type":"long"
		},
		{
			"name":"c_down_packets",
			"type":"long"
		},
		{
			"name":"c_down_bytes",
			"type":"long"
		},
		{
			"name":"c_c2s_packet_jitter",
			"type":"int"
		},
		{
			"name":"c_s2c_packet_jitter",
			"type":"int"
		},
		{
			"name":"c_log_time",
			"type":"long"
		},
		{
			"name":"c_app_type",
			"type":"string"
		},
		{
			"name":"c_stream_time",
			"type":"long"
		},
		{
			"name":"c_hostr",
			"type":"string"
		},
		{
			"name":"c_s_boundary",
			"type":"long"
		},
		{
			"name":"c_s_region",
			"type":"long"
		},
		{
			"name":"c_s_city",
			"type":"long"
		},
		{
			"name":"c_s_district",
			"type":"long"
		},
		{
			"name":"c_s_operators",
			"type":"long"
		},
		{
			"name":"c_s_owner",
			"type":"string"
		},
		{
			"name":"c_d_boundary",
			"type":"long"
		},
		{
			"name":"c_d_region",
			"type":"long"
		},
		{
			"name":"c_d_city",
			"type":"long"
		},
		{
			"name":"c_d_district",
			"type":"long"
		},
		{
			"name":"c_d_operators",
			"type":"long"
		},
		{
			"name":"c_d_owner",
			"type":"string"
		},
		{
			"name":"c_s_mark1",
			"type":"long"
		},
		{
			"name":"c_s_mark2",
			"type":"long"
		},
		{
			"name":"c_s_mark3",
			"type":"long"
		},
		{
			"name":"c_s_mark4",
			"type":"long"
		},
		{
			"name":"c_s_mark5",
			"type":"long"
		},
		{
			"name":"c_d_mark1",
			"type":"long"
		},
		{
			"name":"c_d_mark2",
			"type":"long"
		},
		{
			"name":"c_d_mark3",
			"type":"long"
		},
		{
			"name":"c_d_mark4",
			"type":"long"
		},
		{
			"name":"c_d_mark5",
			"type":"long"
		}
	]
}`

type DataRow struct {
	C_netnum            int32  `avro:"c_netnum"`
	C_ip                int64  `avro:"c_ip"`
	C_flowid            string `avro:"c_flowid"`
	C_src_ipv4          int64  `avro:"c_src_ipv4"`
	C_src_ipv6          []byte `avro:"c_src_ipv6"`
	C_src_port          int32  `avro:"c_src_port"`
	C_s_tunnel_ip       int64  `avro:"c_s_tunnel_ip"`
	C_s_tunnel_port     int32  `avro:"c_s_tunnel_port"`
	C_dest_ipv4         int64  `avro:"c_dest_ipv4"`
	C_dest_ipv6         []byte `avro:"c_dest_ipv6"`
	C_dest_port         int32  `avro:"c_dest_port"`
	C_d_tunnel_ip       int64  `avro:"c_d_tunnel_ip"`
	C_d_tunnel_port     int32  `avro:"c_d_tunnel_port"`
	C_packet_group      int32  `avro:"c_packet_group"`
	C_proto_type        int32  `avro:"c_proto_type"`
	C_connect_status    int32  `avro:"c_connect_status"`
	C_direct            int32  `avro:"c_direct"`
	C_server_dir        int32  `avro:"c_server_dir"`
	C_up_packets        int64  `avro:"c_up_packets"`
	C_up_bytes          int64  `avro:"c_up_bytes"`
	C_down_packets      int64  `avro:"c_down_packets"`
	C_down_bytes        int64  `avro:"c_down_bytes"`
	C_c2s_packet_jitter int32  `avro:"c_c2s_packet_jitter"`
	C_s2c_packet_jitter int32  `avro:"c_s2c_packet_jitter"`
	C_log_time          int64  `avro:"c_log_time"`
	C_app_type          string `avro:"c_app_type"`
	C_stream_time       int64  `avro:"c_stream_time"`
	C_hostr             string `avro:"c_hostr"`
	C_s_boundary        int64  `avro:"c_s_boundary"`
	C_s_region          int64  `avro:"c_s_region"`
	C_s_city            int64  `avro:"c_s_city"`
	C_s_district        int64  `avro:"c_s_district"`
	C_s_operators       int64  `avro:"c_s_operators"`
	C_s_owner           string `avro:"c_s_owner"`
	C_d_boundary        int64  `avro:"c_d_boundary"`
	C_d_region          int64  `avro:"c_d_region"`
	C_d_city            int64  `avro:"c_d_city"`
	C_d_district        int64  `avro:"c_d_district"`
	C_d_operators       int64  `avro:"c_d_operators"`
	C_d_owner           string `avro:"c_d_owner"`
	C_s_mark1           int64  `avro:"c_s_mark1"`
	C_s_mark2           int64  `avro:"c_s_mark2"`
	C_s_mark3           int64  `avro:"c_s_mark3"`
	C_s_mark4           int64  `avro:"c_s_mark4"`
	C_s_mark5           int64  `avro:"c_s_mark5"`
	C_d_mark1           int64  `avro:"c_d_mark1"`
	C_d_mark2           int64  `avro:"c_d_mark2"`
	C_d_mark3           int64  `avro:"c_d_mark3"`
	C_d_mark4           int64  `avro:"c_d_mark4"`
	C_d_mark5           int64  `avro:"c_d_mark5"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func RandStr(n int) string {
	buf := make([]byte, n)
	rand.Read(buf)
	out := hex.EncodeToString(buf)
	return out
}

func RandIPv4() int64 {
	out := rand.New(rand.NewSource(time.Now().UnixNano())).Uint32()
	return int64(out)
}

func RandIPv6String() string {
	size := 16
	ip := make([]byte, size)
	for i := 0; i < size; i++ {
		ip[i] = byte(rand.Intn(256))
	}
	return net.IP(ip).To16().String()
}

func IPv6ToInt(in string) []byte {
	out := net.ParseIP(in)
	return out
}

func RandIPv6() []byte {
	ipStr := RandIPv6String()
	return IPv6ToInt(ipStr)
}

func RandInt64(max int64) int64 {
	data := time.Now().UnixNano()
	if max == 0 {
		return data
	}
	out := data % max
	return out
}

func RandInt32(max int32) int32 {
	data := int32(time.Now().Nanosecond())
	if max == 0 {
		return data
	}
	out := data % max
	return out
}

func Int2Ipv4(i int64) string {
	return net.IP{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}.String()
}

func Bytes2Ipv6(in []byte) string {
	ip := net.IP(in)
	return ip.String()
}

func NewDataRow() *DataRow {
	buf := &DataRow{
		C_netnum:            RandInt32(512),
		C_ip:                RandIPv4(),
		C_flowid:            RandStr(10),
		C_src_ipv4:          RandIPv4(),
		C_src_ipv6:          RandIPv6(),
		C_src_port:          RandInt32(65536),
		C_s_tunnel_ip:       RandIPv4(),
		C_s_tunnel_port:     RandInt32(1024),
		C_dest_ipv4:         RandIPv4(),
		C_dest_ipv6:         RandIPv6(),
		C_dest_port:         RandInt32(65536),
		C_d_tunnel_ip:       RandIPv4(),
		C_d_tunnel_port:     RandInt32(65536),
		C_packet_group:      RandInt32(256),
		C_proto_type:        RandInt32(128),
		C_connect_status:    RandInt32(32),
		C_direct:            RandInt32(512),
		C_server_dir:        RandInt32(256),
		C_up_packets:        RandInt64(0),
		C_up_bytes:          RandInt64(0),
		C_down_packets:      RandInt64(0),
		C_down_bytes:        RandInt64(0),
		C_c2s_packet_jitter: RandInt32(65536),
		C_s2c_packet_jitter: RandInt32(65536),
		C_log_time:          time.Now().UnixMilli(),
		C_app_type:          RandStr(9),
		C_stream_time:       time.Now().UnixMilli(),
		C_hostr:             RandStr(22),
		C_s_boundary:        RandInt64(1024),
		C_s_region:          RandInt64(512),
		C_s_city:            RandInt64(2048),
		C_s_district:        RandInt64(1024),
		C_s_operators:       RandInt64(128),
		C_s_owner:           RandStr(12),
		C_d_boundary:        RandInt64(1024),
		C_d_region:          RandInt64(512),
		C_d_city:            RandInt64(2048),
		C_d_district:        RandInt64(1024),
		C_d_operators:       RandInt64(128),
		C_d_owner:           RandStr(12),
		C_s_mark1:           RandInt64(65536),
		C_s_mark2:           RandInt64(65536),
		C_s_mark3:           RandInt64(65536),
		C_s_mark4:           RandInt64(65536),
		C_s_mark5:           RandInt64(65536),
		C_d_mark1:           RandInt64(65536),
		C_d_mark2:           RandInt64(65536),
		C_d_mark3:           RandInt64(65536),
		C_d_mark4:           RandInt64(65536),
		C_d_mark5:           RandInt64(65536),
	}
	return buf
}

func Write2Avro(bucketSize int) *bytes.Buffer {
	buffer := &bytes.Buffer{}
	schema := avro.MustParseSchema(DataSchema)
	writer := avro.NewGenericDatumWriter()
	writer.SetSchema(schema)
	encoder := avro.NewBinaryEncoder(buffer)

	for i := 0; i < bucketSize; i++ {
		ptrData := NewDataRow()
		data := *ptrData
		record := avro.NewGenericRecord(schema)
		value := reflect.ValueOf(data)
		typ := reflect.TypeOf(data)
		for j := 0; j < value.NumField(); j++ {
			tag := typ.Field(j).Tag.Get("avro")
			val := value.Field(j).Interface()
			record.Set(tag, val)
			log.Tracef("Set avro field %s: %v", tag, val)
		}
		writer.Write(record, encoder)
	}
	return buffer
}

func Write2Csv(bucketSize int) *bytes.Buffer {
	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)
	var records [][]string
	for i := 0; i < bucketSize; i++ {
		ptrData := NewDataRow()
		data := *ptrData
		value := reflect.ValueOf(data)

		var line []string
		for j := 0; j < value.NumField(); j++ {
			kind := value.Field(j).Kind()
			name := value.Type().Field(j).Name
			fieldValue := value.Field(j).Interface()
			switch kind {

			case reflect.Int32:
				v := fmt.Sprintf("%v", fieldValue.(int32))
				line = append(line, v)
				log.Tracef("%s match int32 item %d, val: %s", name, j, v)
			case reflect.Int64:
				// ipv4 addr
				if strings.HasSuffix(name, "_ip") || strings.HasSuffix(name, "_ipv4") {
					v := Int2Ipv4(fieldValue.(int64))
					line = append(line, v)
					log.Tracef("%s match int64-ipv4 item %d, val: %s", name, j, v)
				} else {
					v := fmt.Sprintf("%v", fieldValue.(int64))
					line = append(line, v)
					log.Tracef("%s match int64 item %d, val: %s", name, j, v)
				}
			case reflect.String:
				v := fieldValue.(string)
				line = append(line, v)
				log.Tracef("%s match string item %d, val: %s", name, j, v)
			case reflect.Slice:
				// ipv6 address
				tmp := fieldValue.([]byte)
				v := Bytes2Ipv6(tmp)
				line = append(line, v)
				log.Tracef("%s match slice item %d, val: %s", name, j, v)
			default:
				v := "0"
				line = append(line, v)
				log.Tracef("%s match default item %d, val: %s", name, j, v)
			}

		}
		records = append(records, line)
	}
	writer.WriteAll(records)
	return buffer
}

func PushMessage(conf *Config, ptrMap *map[string]*chan *bytes.Buffer) {
	pipMap := *ptrMap
	var msg []byte
	bufSize := conf.MessageSize
	if conf.DataFmt == "avro" {
		buffer := Write2Avro(bufSize)
		msg = buffer.Bytes()
		buffer.Reset()
	} else {
		buffer := Write2Csv(bufSize)
		msg = buffer.Bytes()
		buffer.Reset()
	}

	msgSize := len(msg)
	for topic, ptrPipe := range pipMap {
		newBuffer := bytes.NewBuffer(msg)
		//newBuffer.Write(msg)
		pipe := *ptrPipe
		pipe <- newBuffer
		log.Debugf("Made %v (bytes) data for topic %s", msgSize, topic)
	}

}

func sentByCli(conf *Config, buf *bytes.Buffer, h interface{}, chanOut *chan *Statistician) {
	switch t := h.(type) {
	case *HttpHandler:
		h.(*HttpHandler).Do(conf, buf, chanOut)
	case *KafkaHandler:
		h.(*KafkaHandler).Do(conf, buf, chanOut)
	default:
		log.Fatalf("Unknow handler type %T", t)
	}
	buf.Reset()
}

func SendMessage(conf *Config, ptrPipe *chan *bytes.Buffer, h interface{}, chanOut *chan *Statistician) {
	buf, ok := <-*ptrPipe
	if ok {
		sentByCli(conf, buf, h, chanOut)
		buf.Reset()
	}
}
