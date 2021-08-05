/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bytes"
	"compress/zlib"
	"kafkacli/src/client"
	"log"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/golang/snappy"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "同步生产一条消息",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(viper.GetString("KAFKA.TOPIC")) == 0 {
			log.Fatalf("not config topic")
			return
		}

		if len(viper.GetString("KAFKA.MESSAGE")) == 0 {
			log.Fatalf("not config message")
			return
		}

		if viper.GetBool("LOGGER.VERBOSE") {
			client.SetLogger()
		}

		c := sarama.NewConfig()
		KafkaVersion, err := sarama.ParseKafkaVersion(
			viper.GetString("KAFKA.VERSION"),
		)
		if err != nil {
			log.Fatalf("parse kafka version failed: %s", err.Error())
			return
		}
		c.Version = KafkaVersion
		c.Producer.RequiredAcks = sarama.WaitForAll
		c.Producer.Return.Successes = true
		c.Producer.Return.Errors = true
		c.Producer.Partitioner = sarama.NewRandomPartitioner

		producer, err := sarama.NewAsyncProducer(
			viper.GetStringSlice("KAFKA.ADDRESSES"),
			c,
		)
		if err != nil {
			log.Fatalf("new kafka client failed: %s", err.Error())
			return
		}
		defer producer.Close()

		var message string

		switch viper.GetString("KAFKA.COMPRESS") {
		case "gz":
			var in bytes.Buffer
			b := []byte(viper.GetString("KAFKA.MESSAGE"))
			w := zlib.NewWriter(&in)
			w.Write(b)
			w.Close()
			message = in.String()
		case "snappy":
			message = string(snappy.Encode(nil,
				[]byte(viper.GetString("KAFKA.MESSAGE"))))
		default:
			message = viper.GetString("KAFKA.MESSAGE")
		}

		tb, err := gotable.Create("topic", "partition", "offset",
			"message", "compress", "send ok", "err")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		row := make(map[string]string)
		row["topic"] = viper.GetString("KAFKA.TOPIC")
		row["message"] = viper.GetString("KAFKA.MESSAGE")
		row["compress"] = viper.GetString("KAFKA.COMPRESS")

		producer.Input() <- &sarama.ProducerMessage{
			Topic: viper.GetString("KAFKA.TOPIC"),
			Key:   nil,
			Value: sarama.StringEncoder(message),
		}

		select {
		case resp := <-producer.Successes():
			row["send ok"] = "true"
			row["partition"] = strconv.Itoa(int(resp.Partition))
			row["offset"] = strconv.FormatInt(resp.Offset, 10)
		case resp := <-producer.Errors():
			row["send ok"] = "false"
			row["err"] = resp.Error()
		}

		tb.AddRow(row)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
