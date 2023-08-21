package option

import (
	"os"

	goflag "flag"

	flag "github.com/spf13/pflag"
)

type Options struct {
	*TLSConfig
	KubeConfig           string
	Broker               string
	QoS                  byte
	ClientID             string
	Retained             bool
	ProviderSendTopic    string
	ProviderReceiveTopic string
	InformerSendTopic    string
	InformerReceiveTopic string
	ClusterName          string
	ReceiveTopic         string
	SendTopic            string
}

type TLSConfig struct {
	EnableTLS  bool
	CACert     string
	ClientCert string
	ClientKey  string
}

func ParseOptionFromFlag() *Options {
	opt := &Options{
		TLSConfig: &TLSConfig{},
	}
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.StringVarP(&opt.KubeConfig, "kubeconfig", "k", "", "the kubeconfig for apiserver")
	flag.StringVarP(&opt.Broker, "broker", "b", "", "the MQTT server")
	flag.BoolVarP(&opt.EnableTLS, "tls", "", false, "whether to enable the TLS connection")
	flag.StringVarP(&opt.CACert, "ca-crt", "", "", "the ca certificate path")
	flag.StringVarP(&opt.ClientCert, "client-crt", "", "", "the client certificate path")
	flag.StringVarP(&opt.ClientKey, "client-key", "", "", "the client key path")
	flag.StringVarP(&opt.ClientID, "client-id", "", "sender", "the client id for the MQTT")
	flag.StringVarP(&opt.ProviderSendTopic, "provider-send", "", "", "the topic for provider send payload")
	flag.StringVarP(&opt.ProviderReceiveTopic, "provider-receive", "", "", "the topic for provider receive payload")
	flag.StringVarP(&opt.InformerSendTopic, "informer-send", "", "", "the topic for informer send payload")
	flag.StringVarP(&opt.InformerReceiveTopic, "informer-receive", "", "", "the topic for informer receive payload")
	flag.StringVarP(&opt.SendTopic, "send-topic", "", "", "the topic for send payload")
	flag.StringVarP(&opt.ReceiveTopic, "receive-topic", "", "", "the topic for receive payload")
	flag.StringVarP(&opt.ClusterName, "cluster", "", "hub", "the cluster where the syncer is running ")
	QoS := flag.IntP("QoS", "q", 0,
		"the level of reliability and assurance of message delivery between an MQTT client and broker")
	flag.BoolVarP(&opt.Retained, "retained", "", false, "retain the MQTT message or not")

	flag.Parse()
	opt.QoS = byte(*QoS)
	if opt.Broker == "" {
		opt.Broker = os.Getenv("BROKER")
	}
	if opt.KubeConfig == "" {
		opt.KubeConfig = os.Getenv("KUBECONFIG")
	}
	return opt
}
