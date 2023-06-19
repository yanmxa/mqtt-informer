package config

type SendConfig struct {
	*TLSConfig
	Broker   string
	ClientID string
	Topic    string
	QoS      byte
	Retained bool
}

func NewSendConfig() SendConfig {
	return SendConfig{
		TLSConfig: &TLSConfig{},
	}
}

type ReceiveConfig struct {
	*TLSConfig
	Broker   string
	ClientID string
	Topic    string
	QoS      byte
}

func NewReceiveConfig() ReceiveConfig {
	return ReceiveConfig{
		TLSConfig: &TLSConfig{},
	}
}

type TLSConfig struct {
	EnableTLS  bool
	CACert     string
	ClientCert string
	ClientKey  string
}
