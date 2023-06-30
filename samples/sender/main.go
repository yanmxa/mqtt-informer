package main

import (
	"fmt"
	"time"

	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/transport"
)

func main() {
	opt := option.ParseOptionFromFlag()
	transporter := transport.NewMqttTransport(opt)
	c := transporter.GetClient()

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Sample Publisher Started")
	i := 0
	for range time.Tick(time.Duration(1) * time.Second) {
		if i == 5 {
			break
		}
		text := fmt.Sprintf("this is msg #%d!", i)
		token := c.Publish(opt.PayloadTopic, opt.QoS, opt.Retained, text)
		token.Wait()
		if token.Error() != nil {
			fmt.Println(token.Error())
			break
		}
		fmt.Println("Published: ", text)
		i++
	}
	c.Disconnect(250)
}
