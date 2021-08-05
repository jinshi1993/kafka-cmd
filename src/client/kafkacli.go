package client

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func SetLogger() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

func NewKafkaClient(version string, addresses []string) (sarama.Client, error) {
	var err error

	c := sarama.NewConfig()
	c.Version, err = sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, err
	}

	return sarama.NewClient(addresses[:], c)
}
