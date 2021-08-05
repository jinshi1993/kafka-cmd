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
	"sort"

	"github.com/Shopify/sarama"
	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// consumergroupsCmd represents the consumergroups command
var consumergroupsCmd = &cobra.Command{
	Use:   "consumergroups",
	Short: "列出所有消费者组（0.8版本存储在zookeeper中的消费者组信息将不再展示）",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
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

		consumerGroups := []string{}
		for _, broker := range KafkaCli.Brokers() {
			if ok, _ := broker.Connected(); !ok {
				if err := broker.Open(KafkaCli.Config()); err != nil {
					log.Fatalf("Cannot connect to broker[%d] %s: %s",
						broker.ID(), broker.Addr(), err.Error())
					return
				}
			}
			defer broker.Close()

			groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				log.Fatalf("Cannot get consumer group: %s", err.Error())
				return
			}

			for groupName := range groups.Groups {
				consumerGroups = append(consumerGroups, groupName)
			}
		}
		sort.Strings(consumerGroups)

		tb, err := gotable.Create("consumer groups")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		rows := make([]map[string]string, 0)

		for _, consumerGroup := range consumerGroups {
			row := make(map[string]string)
			row["consumer group"] = consumerGroup
			rows = append(rows, row)
		}

		tb.AddRows(rows)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(consumergroupsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumergroupsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumergroupsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
