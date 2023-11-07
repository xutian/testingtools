package utils

import (
	"bytes"
	//"runtime"
	//"context"
	"sync"
	"time"

	"github.com/panjf2000/ants"
	log "github.com/sirupsen/logrus"
	//"golang.org/x/time/rate"
)

func Consumer4Topics(conf *Config, ptrChanStatis *chan *Statistician, ptrMapChanPipes *map[string]*chan *bytes.Buffer, ptrMapReports *map[string]*Report, ptrCtlChans *map[string]*<-chan time.Time, poolSize int) {
	var wg sync.WaitGroup

	topicNum := len(conf.Topics)
	mapChanPipes := *ptrMapChanPipes
	mapReports := *ptrMapReports
	chanStatis := *ptrChanStatis
	ctlChans := *ptrCtlChans

	wg.Add(topicNum)
	for _, topic := range conf.Topics {
		ctlChan := ctlChans[topic]
		go func(topic string) {
			var pipe = mapChanPipes[topic]
			DataConsumer(conf, topic, ptrChanStatis, pipe, ctlChan, poolSize)
			mapReports[topic].EndTime = time.Now()
			wg.Done()
			log.Debugf("Test done for topic %s!", topic)
		}(topic)
	}
	wg.Wait()
	//Wait Consumer goroutine for all topic done
	log.Debugln("Test done for all topics")
	//Close statis channel to finish Calc goroutine,
	//If not close the goroutine main goroutine blocked
	close(chanStatis)

}

func DataProducer(conf *Config, mpPipe *map[string]*chan *bytes.Buffer, ptrCtlChan *<-chan time.Time, poolSize int) {
	//// create task pool to generate data and sent data to channel
	var wg sync.WaitGroup
	pool, err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			PushMessage(conf, mpPipe)
			wg.Done()
		})

	if err != nil {
		log.Fatalf("Create task pool for make data failed with error %v", err)
	}
	defer pool.Release()
	log.Debugln("Begin to product data...")
	ctlChan := *ptrCtlChan
	if conf.RunTimeout > 0 {
		for {
			select {
			case <-ctlChan:
				log.Println("Get done signal, stop!")
				goto ForEnd
				// for _, topic := range conf.Topics {
				// 	pipe := (*mpPipe)[topic]
				// 	close(*pipe)
				// }
				// runtime.Goexit()
			default:
				wg.Add(1)
				pool.Invoke(1)
			}
		}
	} else {
		for i := 0; i < conf.MessageNum; i++ {
			wg.Add(1)
			pool.Invoke(1)
		}
	}
ForEnd:
	wg.Wait()
	for _, topic := range conf.Topics {
		ch := (*mpPipe)[topic]
		close(*ch)
	}
	log.Debugln("Put data to channel done!")
}

func DataConsumer(conf *Config, topic string, out *chan *Statistician, pipe *chan *bytes.Buffer, ptrCtlChan *<-chan time.Time, poolSize int) {

	var wg sync.WaitGroup
	// New pool for send data
	pool, err := ants.NewPoolWithFunc(
		poolSize,
		func(i interface{}) {
			if conf.MethodId == 1 {
				handler := NewHttpHandler(topic, conf)
				SendMessage(conf, pipe, handler, out)
			} else {
				handler := NewKafkaHandler(topic, conf)
				SendMessage(conf, pipe, handler, out)
			}
			wg.Done()
		})
	if err != nil {
		log.Fatalf("Create task pool for consume data failed with error %v", err)
	}
	defer pool.Release()

	log.Debugln("Begin to consum data...")
	ctlChan := *ptrCtlChan
	if conf.RunTimeout > 0 {
		for {
			select {
			case <-ctlChan:
				log.Println("Get done signal, stop!")
				goto ForEnd
				// for _, topic := range conf.Topics {
				// 	pipe := (*mpPipe)[topic]
				// 	close(*pipe)
				// }
				// runtime.Goexit()
			default:
				wg.Add(1)
				pool.Invoke(1)
			}
		}
	} else {
		for i := 0; i < conf.MessageNum; i++ {
			wg.Add(1)
			pool.Invoke(1)
		}
	}
ForEnd:
	wg.Wait()
	log.Debugln("Sent data Done!")
}
