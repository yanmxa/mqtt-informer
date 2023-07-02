package transport

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"

	"github.com/eclipse/paho.golang/paho"
	"github.com/yanmxa/transport-informer/pkg/apis"
	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/utils"
	"k8s.io/klog/v2"
)

var _ Transport = (*mqttTransport)(nil)

type mqttTransport struct {
	ctx       context.Context
	client    *paho.Client
	qos       byte
	retained  bool
	receivers map[string]Receiver
}

func NewMqttTransport(ctx context.Context, opt *option.Options) *mqttTransport {
	var conn net.Conn
	var err error
	if opt.EnableTLS {
		tlsConfig := utils.NewTLSConfig(opt.CACert, opt.ClientCert, opt.ClientKey)
		conn, err = tls.Dial("tcp", opt.Broker, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", opt.Broker)
	}
	if err != nil {
		panic(err)
	}

	conf := paho.ClientConfig{
		ClientID: opt.ClientID,
		Conn:     conn,
	}
	client := paho.NewClient(conf)

	cp := &paho.Connect{
		KeepAlive:  30,
		ClientID:   opt.ClientID,
		CleanStart: true,
	}

	connAck, err := client.Connect(ctx, cp)
	if err != nil {
		panic(err)
	}
	if connAck.ReasonCode != 0 {
		klog.Errorf("Failed to connect to %s: - %s", opt.Broker, connAck.Properties.ReasonString)
		panic(err)
	}
	klog.Info("Connected to ", opt.Broker)

	return &mqttTransport{
		ctx:       ctx,
		client:    client,
		qos:       opt.QoS,
		retained:  opt.Retained,
		receivers: make(map[string]Receiver),
	}
}

func (t *mqttTransport) Send(topic string, msg apis.TransportMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = t.client.Publish(t.ctx, &paho.Publish{
		QoS:     t.qos,
		Topic:   topic,
		Payload: payload,
		Retain:  t.retained,
	})
	if err != nil {
		return err
	}
	return nil
}

// start a goroutine to receive message from subscribed topic
func (t *mqttTransport) Receive(topic string) (Receiver, error) {
	if t.receivers[topic] != nil {
		klog.Info("receiver(%s) already existed!", topic)
		return t.receivers[topic], nil
	}

	messageChan := make(chan apis.TransportMessage)
	t.receivers[topic] = NewDefaultReceiver(messageChan)

	t.client.Router.RegisterHandler(topic, func(msg *paho.Publish) {
		transportMsg := &apis.TransportMessage{}
		err := json.Unmarshal(msg.Payload, transportMsg)
		if err != nil {
			klog.Errorf("failed to unmarshal message: %s", err.Error())
			return
		}
		klog.Infof("received message(%s): %s", transportMsg.ID, transportMsg.Type)
		messageChan <- *transportMsg
	})

	if _, err := t.client.Subscribe(t.ctx, &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			topic: {QoS: t.qos, NoLocal: true, RetainAsPublished: t.retained},
		},
	}); err != nil {
		return nil, err
	}

	klog.Infof("receiver(%s) started!", topic)
	return t.receivers[topic], nil
}

// func (t *mqttTransport) waitUntilConnected() error {
// 	if !t.client.IsConnected() {
// 		if token := t.client.Connect(); token.Wait() && token.Error() != nil {
// 			return token.Error()
// 		}
// 	}
// 	return nil
// }

func (t *mqttTransport) GetClient() *paho.Client {
	return t.client
}

func (t *mqttTransport) Stop() {
	for topic, receiver := range t.receivers {
		receiver.Stop()
		klog.Infof("transport receiver(%s) stopped!", topic)
	}

	err := t.client.Conn.Close()
	if err != nil {
		klog.Error(err)
	}
	klog.Info("transport is disconnected!")
}
