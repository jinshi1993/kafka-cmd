package client

import (
	"log"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("LOGGER.VERBOSE", false)
	viper.SetDefault("KAFKA.VERSION", sarama.V0_10_2_1.String())
	viper.SetDefault(
		"KAFKA.ADDRESSES",
		[]string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
			"127.0.0.1:9093",
		},
	)
	viper.SetDefault("KAFKA.TOPIC", "cloudfastline_bolebuf_forward")
	viper.SetDefault("KAFKA.CONSUMERGROUP", "bolebuf_realtime_g")
}

func Test_1(t *testing.T) {
	if viper.GetBool("LOGGER.VERBOSE") {
		SetLogger()
	}

	KafkaCli, err := NewKafkaClient(
		viper.GetString("KAFKA.VERSION"),
		viper.GetStringSlice("KAFKA.ADDRESSES"),
	)
	if err != nil {
		log.Fatalf("new kafka client failed: %s", err.Error())
		return
	}
	defer KafkaCli.Close()

	partitions, err := KafkaCli.Partitions(
		viper.GetString("KAFKA.TOPIC"),
	)
	if err != nil {
		log.Fatalf("Cannot get partitions of topic %s: %s",
			viper.GetString("KAFKA.TOPIC"),
			err.Error(),
		)
		return
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(
		viper.GetString("KAFKA.CONSUMERGROUP"),
		KafkaCli,
	)
	if err != nil {
		log.Fatalf("failed to create offset manager for topic=%s, group=%s err=%s",
			viper.GetString("KAFKA.TOPIC"),
			viper.GetString("KAFKA.CONSUMERGROUP"),
			err.Error())
		return
	}

	for _, partition := range partitions {
		partitionOffsetManager, err := offsetManager.ManagePartition(
			viper.GetString("KAFKA.TOPIC"),
			partition,
		)
		if err != nil {
			log.Fatalf("failed to get partition manager for topic=%s, partition=%d, err=%s",
				viper.GetString("KAFKA.TOPIC"),
				partition, err.Error(),
			)
			continue
		}
		cgOffset, _ := partitionOffsetManager.NextOffset()

		pOffset, err := KafkaCli.GetOffset(
			viper.GetString("KAFKA.TOPIC"),
			partition,
			sarama.OffsetNewest,
		)
		if err != nil {
			log.Fatalf("failed to get partition offset for topic=%s, partition=%d, err=%s",
				viper.GetString("KAFKA.TOPIC"),
				partition, err.Error(),
			)
			continue
		}

		log.Printf("%d %d", cgOffset, pOffset)
	}
}
