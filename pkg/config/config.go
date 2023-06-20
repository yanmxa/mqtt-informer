package config

import (
	"os"

	goflag "flag"

	flag "github.com/spf13/pflag"
)

var (
	ReceiveTopic string
	SendTopic    string
	QoS          = byte(0)
	Retained     = false
)

type BaseConfig struct {
	*TLSConfig
	KubeConfig string
	Broker     string
	Topic      string
	QoS        byte
}

type SendConfig struct {
	*BaseConfig
	SenderID string
	Retained bool
}

func NewSendConfig() SendConfig {
	return SendConfig{
		BaseConfig: &BaseConfig{
			TLSConfig: &TLSConfig{},
		},
	}
}

type ReceiveConfig struct {
	*BaseConfig
	ReceiverID string
}

func NewReceiveConfig() ReceiveConfig {
	return ReceiveConfig{
		BaseConfig: &BaseConfig{
			TLSConfig: &TLSConfig{},
		},
	}
}

type TLSConfig struct {
	EnableTLS  bool
	CACert     string
	ClientCert string
	ClientKey  string
}

func SetReceiveTopic(topic string) {
	ReceiveTopic = topic
}

func SetSendTopic(topic string) {
	SendTopic = topic
}

func SetQoS(qos byte) {
	QoS = qos
}

func SetRetained(retain bool) {
	Retained = retain
}

func GetConfigs() (*SendConfig, *ReceiveConfig) {
	var senderId, receiverId string
	var retained bool
	baseConfig := &BaseConfig{}
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringVarP(&baseConfig.KubeConfig, "kubeconfig", "k", "", "the kubeconfig for apiserver")
	flag.StringVarP(&baseConfig.Broker, "broker", "b", "", "the MQTT server")
	flag.BoolVarP(&baseConfig.EnableTLS, "tls", "", false, "whether to enable the TLS connection")
	flag.StringVarP(&baseConfig.CACert, "ca-crt", "", "", "the ca certificate path")
	flag.StringVarP(&baseConfig.ClientCert, "client-crt", "", "", "the client certificate path")
	flag.StringVarP(&baseConfig.ClientKey, "client-key", "", "", "the client key path")
	flag.StringVarP(&receiverId, "receiver-id", "", "receiver", "the client id for the MQTT consumer")
	flag.StringVarP(&senderId, "sender-id", "", "sender", "the client id for the MQTT producer")
	flag.StringVarP(&baseConfig.Topic, "topic", "", "", "the topic for the MQTT consumer")
	QoS := flag.IntP("QoS", "q", 0,
		"the level of reliability and assurance of message delivery between an MQTT client and broker")
	flag.BoolVarP(&retained, "retained", "", false, "retain the MQTT message or not")

	flag.Parse()
	baseConfig.QoS = byte(*QoS)
	if baseConfig.Broker == "" {
		baseConfig.Broker = os.Getenv("BROKER")
	}
	if baseConfig.KubeConfig == "" {
		baseConfig.KubeConfig = os.Getenv("KUBECONFIG")
	}

	senderConfig := &SendConfig{
		BaseConfig: baseConfig,
		SenderID:   senderId,
		Retained:   retained,
	}

	receiverConfig := &ReceiveConfig{
		BaseConfig: baseConfig,
		ReceiverID: receiverId,
	}

	SetReceiveTopic(baseConfig.Topic)
	SetSendTopic(baseConfig.Topic)
	SetRetained(senderConfig.Retained)
	SetQoS(baseConfig.QoS)

	return senderConfig, receiverConfig
}
