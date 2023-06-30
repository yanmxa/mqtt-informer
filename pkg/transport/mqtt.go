package transport

import (
	"encoding/json"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/yanmxa/transport-informer/pkg/apis"
	"github.com/yanmxa/transport-informer/pkg/option"
	"github.com/yanmxa/transport-informer/pkg/utils"
	"k8s.io/klog/v2"
)

var _ Transport = (*mqttTransport)(nil)

type mqttTransport struct {
	client       MQTT.Client
	receiveTopic string
	sendTopic    string
	qos          byte
	retained     bool
	receiver     Receiver
}

func NewMqttTransport(opt *option.Options) *mqttTransport {
	clientOpts := MQTT.NewClientOptions()
	clientOpts.AddBroker(opt.Broker)
	clientOpts.SetClientID(opt.ClientID)
	clientOpts.SetAutoReconnect(true)
	if opt.EnableTLS {
		clientOpts.SetTLSConfig(utils.NewTLSConfig(opt.CACert, opt.ClientCert, opt.ClientKey))
	}
	return &mqttTransport{
		client:       MQTT.NewClient(clientOpts),
		receiveTopic: opt.ReceiveTopic,
		sendTopic:    opt.SendTopic,
		qos:          opt.QoS,
		retained:     opt.Retained,
	}
}

func (t *mqttTransport) Send(msg apis.TransportMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	t.waitUntilConnected()
	token := t.client.Publish(t.sendTopic, t.qos, t.retained, payload)
	token.Wait()
	return token.Error()
}

// start a goroutine to receive message from subscribed topic
func (t *mqttTransport) Receive() (Receiver, error) {
	err := t.waitUntilConnected()
	if err != nil {
		return nil, err
	}
	if t.receiver != nil {
		klog.Info("receiver(%s) already started!", t.receiveTopic)
		return t.receiver, nil
	}
	messageChan := make(chan apis.TransportMessage)
	t.receiver = NewDefaultReceiver(messageChan)
	if token := t.client.Subscribe(t.receiveTopic, t.qos, func(client MQTT.Client, msg MQTT.Message) {
		transportMsg := &apis.TransportMessage{}
		err := json.Unmarshal(msg.Payload(), transportMsg)
		if err != nil {
			klog.Error(err)
			return
		}
		klog.Infof("received message: %s - %s", transportMsg.ID, transportMsg.Type)
		messageChan <- *transportMsg
	}); token.Wait() && token.Error() != nil {
		t.receiver.Stop()
		return nil, token.Error()
	}
	klog.Infof("receiver(%s) started!", t.receiveTopic)
	return t.receiver, nil
}

func (t *mqttTransport) waitUntilConnected() error {
	if !t.client.IsConnected() {
		if token := t.client.Connect(); token.Wait() && token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}

func (t *mqttTransport) GetClient() MQTT.Client {
	return t.client
}

func (t *mqttTransport) Stop() {
	if t.receiver != nil {
		t.receiver.Stop()
		klog.Infof("transport receiver(%s) stopped!", t.receiveTopic)
	}
	if t.client.IsConnected() {
		t.client.Disconnect(250)
		klog.Info("transport client disconnected!")
	}
}
