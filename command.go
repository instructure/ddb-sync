package main

import (
	"fmt"
	"io"
	"os"

	"gerrit.instructure.com/ddb-sync/plan"

	flag "github.com/spf13/pflag"
	"gopkg.in/yaml.v2"
)

func ParseArgs(args []string) ([]plan.Plan, error) {
	flagSet := flagSet()

	if len(args) == 0 {
		fmt.Println("ddb-sync:")
		fmt.Println(flagSet.FlagUsages())
		return nil, fmt.Errorf("Improper usage")
	}

	err := flagSet.Parse(args)
	if err != nil {
		return nil, err
	}

	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("Unknown argument(s): %v", flagSet.Args())
	}

	if file, _ := flagSet.GetString("config-file"); file != "" {
		// Grab some plans from the config file parsing
		plans, err := parseConfigFile(file)
		if err != nil {
			return nil, err
		}
		return plans, nil
	}

	inputRegion, _ := flagSet.GetString("input-region")
	inputTable, _ := flagSet.GetString("input-table")
	inputRole, _ := flagSet.GetString("input-role-arn")

	outputRegion, _ := flagSet.GetString("output-region")
	outputTable, _ := flagSet.GetString("output-table")
	outputRole, _ := flagSet.GetString("output-role-arn")

	backfill, _ := flagSet.GetBool("backfill")
	stream, _ := flagSet.GetBool("stream")

	plan := []plan.Plan{
		{
			Input: plan.Input{
				Region:    inputRegion,
				TableName: inputTable,

				RoleARN: inputRole,
			},
			Output: plan.Output{
				Region:    outputRegion,
				TableName: outputTable,

				RoleARN: outputRole,
			},
			Backfill: plan.Backfill{
				Disabled: !backfill,
			},
			Stream: plan.Stream{
				Disabled: !stream,
			},
		},
	}

	return plan, err
}

func flagSet() *flag.FlagSet {
	flag := flag.NewFlagSet("ddb-sync", flag.ContinueOnError)

	flag.String("config-file", "", "Filename for configuration yaml")

	flag.String("input-region", "", "The input region")
	flag.String("input-table", "", "Name of the input table")
	flag.String("input-role-arn", "", "ARN of the input role")

	flag.String("output-region", "", "The output region")
	flag.String("output-table", "", "Name of the output table")
	flag.String("output-role-arn", "", "ARN of the output role")

	flag.Bool("backfill", true, "Perform the backfill operation")
	flag.Bool("stream", true, "Perform the streaming operation")

	return flag
}

type PlanConfig struct {
	Plan []plan.Plan `yaml:"plan"`
}

func parseConfigFile(filePath string) ([]plan.Plan, error) {
	var f io.Reader
	var err error

	if filePath == "-" {
		f = os.Stdin
	} else {
		fp, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("Failed to open configuration file: %v", err)
		}
		defer fp.Close()
		f = fp
	}

	var config PlanConfig

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse configuration file: %v", err)
	}
	return config.Plan, nil
}
