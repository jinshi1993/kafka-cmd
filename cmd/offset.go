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

// offsetCmd represents the offset command
var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "列出某个topic当前最老、最新offset",
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

		tb, err := gotable.Create("topic", "partition",
			"oldest offset", "newest offset", "messages", "err")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		rows := make([]map[string]string, 0)

		for _, partition := range partitions {
			row := make(map[string]string)
			row["topic"] = viper.GetString("KAFKA.TOPIC")
			row["partition"] = strconv.Itoa(int(partition))

			currentOffset, err := KafkaCli.GetOffset(
				viper.GetString("KAFKA.TOPIC"),
				partition,
				sarama.OffsetNewest)
			if err != nil {
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}

			oldestOffset, err := KafkaCli.GetOffset(
				viper.GetString("KAFKA.TOPIC"),
				partition,
				sarama.OffsetOldest)
			if err != nil {
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}

			row["oldest offset"] = strconv.FormatInt(oldestOffset, 10)
			row["newest offset"] = strconv.FormatInt(currentOffset, 10)
			row["messages"] = strconv.FormatInt(currentOffset-oldestOffset, 10)
			rows = append(rows, row)
		}

		tb.AddRows(rows)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(offsetCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// offsetCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
