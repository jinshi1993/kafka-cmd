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
	"kafkacli/src/client"
	"log"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// lagCmd represents the lag command
var lagCmd = &cobra.Command{
	Use:   "lag",
	Short: "列出某个消费者组的lag情况（需要指定topic）",
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

		if len(viper.GetString("KAFKA.CONSUMERGROUP")) == 0 {
			log.Fatalf("not config consumer group")
			return
		}

		if viper.GetBool("LOGGER.VERBOSE") {
			client.SetLogger()
		}

		KafkaCli, err := client.NewKafkaClient(
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

		tb, err := gotable.Create("topic", "consumer group", "partition",
			"consumer group offset", "newest offset",
			"lag", "err")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		rows := make([]map[string]string, 0)

		for _, partition := range partitions {
			row := make(map[string]string)
			row["topic"] = viper.GetString("KAFKA.TOPIC")
			row["consumer group"] = viper.GetString("KAFKA.CONSUMERGROUP")
			row["partition"] = strconv.Itoa(int(partition))

			partitionOffsetManager, err := offsetManager.ManagePartition(
				viper.GetString("KAFKA.TOPIC"),
				partition,
			)
			if err != nil {
				log.Fatalf("failed to get partition manager for topic=%s, partition=%d, err=%s",
					viper.GetString("KAFKA.TOPIC"),
					partition, err.Error(),
				)
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}
			consumerGroupOffset, _ := partitionOffsetManager.NextOffset()

			currentOffset, err := KafkaCli.GetOffset(
				viper.GetString("KAFKA.TOPIC"),
				partition,
				sarama.OffsetNewest,
			)
			if err != nil {
				log.Fatalf("failed to get partition offset for topic=%s, partition=%d, err=%s",
					viper.GetString("KAFKA.TOPIC"),
					partition, err.Error(),
				)
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}

			row["consumer group offset"] = strconv.FormatInt(consumerGroupOffset, 10)
			row["newest offset"] = strconv.FormatInt(currentOffset, 10)
			row["lag"] = strconv.FormatInt(currentOffset-consumerGroupOffset, 10)
			rows = append(rows, row)
		}

		tb.AddRows(rows)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(lagCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// lagCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// lagCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
