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
	"fmt"
	"kafkacli/src/client"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/liushuochen/gotable"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// replicasCmd represents the replicas command
var replicasCmd = &cobra.Command{
	Use:   "replicas",
	Short: "列出某个topic所有partition的副本，以及ISR",
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
			"replicas", "ISR", "percentage", "err")
		if err != nil {
			log.Fatalf("Create table failed: %s", err.Error())
			return
		}

		rows := make([]map[string]string, 0)

		for _, partition := range partitions {
			row := make(map[string]string)
			row["topic"] = viper.GetString("KAFKA.TOPIC")
			row["partition"] = strconv.Itoa(int(partition))

			var replicasStr, inSyncReplicasStr []string

			replicas, err := KafkaCli.Replicas(
				viper.GetString("KAFKA.TOPIC"),
				partition,
			)
			if err != nil {
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}

			for _, replica := range replicas {
				replicasStr = append(replicasStr, strconv.Itoa(int(replica)))
			}
			sort.Strings(replicasStr)

			inSyncReplicas, err := KafkaCli.InSyncReplicas(
				viper.GetString("KAFKA.TOPIC"),
				partition,
			)
			if err != nil {
				row["err"] = err.Error()
				rows = append(rows, row)
				continue
			}

			for _, inSyncReplica := range inSyncReplicas {
				inSyncReplicasStr = append(inSyncReplicasStr, strconv.Itoa(int(inSyncReplica)))
			}
			sort.Strings(inSyncReplicasStr)

			row["replicas"] = strings.Join(replicasStr, ",")
			row["ISR"] = strings.Join(inSyncReplicasStr, ",")
			row["percentage"] = fmt.Sprintf("%d/%d %d%%",
				len(replicas),
				len(inSyncReplicas),
				len(inSyncReplicas)*100/len(replicas))
			rows = append(rows, row)
		}

		tb.AddRows(rows)
		tb.PrintTable()
	},
}

func init() {
	rootCmd.AddCommand(replicasCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// replicasCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// replicasCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
