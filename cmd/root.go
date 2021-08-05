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
	"os"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "kafkacli",
	Short: "A brief description of your application",
	Long:  `kafka简易命令行工具(v0.0.1)`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	cobra.OnInitialize(initDefault)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	//rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.kafkacli.yaml)")
	rootCmd.PersistentFlags().Bool("verbose", false, "是否打印debug日志")
	rootCmd.PersistentFlags().String("broker-version", sarama.V0_10_2_1.String(), "broker版本号")
	rootCmd.PersistentFlags().StringSlice(
		"broker-addresses",
		[]string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
			"127.0.0.1:9093",
		},
		"broker地址",
	)
	rootCmd.PersistentFlags().String("topic", "", "具体topic名称")
	rootCmd.PersistentFlags().String("consumer-group", "", "具体消费者组名称")
	rootCmd.PersistentFlags().String("produce-message", "", "你想发送的消息")
	rootCmd.PersistentFlags().Int32("consume-partition", -1, "你想消费的具体的partition")
	rootCmd.PersistentFlags().Int64("consume-offset", -1, "你想消费的具体开始的offset，-1是最新，-2是最老，非负数为具体offset")
	rootCmd.PersistentFlags().String("compress", "none", "消息压缩算法，包括gz、snappy、none（无压缩）")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".kafkacli" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".kafkacli")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func initDefault() {
	viper.SetDefault("KAFKA.VERSION", sarama.V0_10_2_1.String())
	viper.SetDefault(
		"KAFKA.ADDRESSES",
		[]string{
			"127.0.0.1:9091",
			"127.0.0.1:9092",
			"127.0.0.1:9093",
		},
	)

	viper.BindPFlag("LOGGER.VERBOSE", rootCmd.PersistentFlags().Lookup("verbose"))
	viper.BindPFlag("KAFKA.VERSION", rootCmd.PersistentFlags().Lookup("broker-version"))
	viper.BindPFlag("KAFKA.ADDRESSES", rootCmd.PersistentFlags().Lookup("broker-addresses"))
	viper.BindPFlag("KAFKA.TOPIC", rootCmd.PersistentFlags().Lookup("topic"))
	viper.BindPFlag("KAFKA.CONSUMERGROUP", rootCmd.PersistentFlags().Lookup("consumer-group"))
	viper.BindPFlag("KAFKA.MESSAGE", rootCmd.PersistentFlags().Lookup("produce-message"))
	viper.BindPFlag("KAFKA.PARTITION", rootCmd.PersistentFlags().Lookup("consume-partition"))
	viper.BindPFlag("KAFKA.OFFSET", rootCmd.PersistentFlags().Lookup("consume-offset"))
	viper.BindPFlag("KAFKA.COMPRESS", rootCmd.PersistentFlags().Lookup("compress"))
}
