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

	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// topicsCmd represents the topics command
var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "列出所有topic",
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

		topics, err := KafkaCli.Topics()
		if err != nil {
			log.Fatalf("Cannot get topics: %s", err.Error())
			return
		}

		sort.Strings(topics)

		tb, err := gotable.Create("topic")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		rows := make([]map[string]string, 0)
		for _, topic := range topics {
			row := make(map[string]string)
			row["topic"] = topic
			rows = append(rows, row)
		}

		tb.AddRows(rows)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(topicsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// topicsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// topicsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
