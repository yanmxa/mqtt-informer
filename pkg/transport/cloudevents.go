package transport

import (
	"context"
	"crypto/tls"
	"net"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eclipse/paho.golang/paho"
	"github.com/yanmxa/straw/pkg/option"
	"github.com/yanmxa/straw/pkg/utils"

	cemqtt "github.com/cloudevents/sdk-go/protocol/mqtt_paho/v2"
)

// define an enum with informer, provider and both values
type TransportType int

const (
	Informer TransportType = iota
	Provider
)

func CloudeventsClient(ctx context.Context, opt *option.Options) (
	client cloudevents.Client, err error,
) {
	var conn net.Conn
	if opt.EnableTLS {
		tlsConfig := utils.NewTLSConfig(opt.CACert, opt.ClientCert, opt.ClientKey)
		conn, err = tls.Dial("tcp", opt.Broker, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", opt.Broker)
	}
	if err != nil {
		return client, err
	}

	p, err := cemqtt.New(ctx,
		&paho.ClientConfig{
			ClientID: opt.ClientID,
			Conn:     conn,
		},
		cemqtt.WithPublish(&paho.Publish{
			Topic: opt.SendTopic,
		}),
		cemqtt.WithSubscribe(&paho.Subscribe{
			Subscriptions: map[string]paho.SubscribeOptions{
				opt.ReceiveTopic: {QoS: 0},
			},
		}),
	)
	if err != nil {
		return client, err
	}
	return cloudevents.NewClient(p, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
}
