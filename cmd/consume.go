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
	"io/ioutil"
	"kafkacli/src/client"
	"log"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/golang/snappy"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "同步消费一条消息",
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

		if viper.GetInt32("KAFKA.PARTITION") < 0 {
			log.Fatalf("not config partition")
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
		c.Consumer.Return.Errors = true

		consumer, err := sarama.NewConsumer(
			viper.GetStringSlice("KAFKA.ADDRESSES"),
			c,
		)
		if err != nil {
			log.Fatalf("new kafka client failed: %s", err.Error())
			return
		}
		defer consumer.Close()

		tb, err := gotable.Create("topic", "partition", "start offset",
			"message", "compress", "recv ok", "err")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		row := make(map[string]string)
		row["topic"] = viper.GetString("KAFKA.TOPIC")
		row["partition"] = strconv.Itoa(int(viper.GetInt32("KAFKA.PARTITION")))
		row["start offset"] = strconv.FormatInt(viper.GetInt64("KAFKA.OFFSET"), 10)
		row["compress"] = viper.GetString("KAFKA.COMPRESS")

		partitionConsumer, err := consumer.ConsumePartition(
			viper.GetString("KAFKA.TOPIC"),
			viper.GetInt32("KAFKA.PARTITION"),
			viper.GetInt64("KAFKA.OFFSET"),
		)
		if err != nil {
			log.Fatalf("generate partition consumer failed: %s", err.Error())
			return
		}

		select {
		case msg := <-partitionConsumer.Messages():
			row["recv ok"] = "true"
			switch viper.GetString("KAFKA.COMPRESS") {
			case "gz":
				r, err := zlib.NewReader(bytes.NewReader(msg.Value))
				if err != nil {
					row["err"] = err.Error()
					break
				}
				result, err := ioutil.ReadAll(r)
				if err != nil {
					row["err"] = err.Error()
					break
				}
				row["message"] = string(result)
			case "snappy":
				result, err := snappy.Decode(nil, msg.Value)
				if err != nil {
					row["err"] = err.Error()
					break
				}
				row["message"] = string(result)
			default:
				row["message"] = string(msg.Value)
			}
		case err := <-partitionConsumer.Errors():
			row["recv ok"] = "false"
			row["err"] = err.Error()
		}

		tb.AddRow(row)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(consumeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
