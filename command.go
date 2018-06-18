package main

import (
	"fmt"

	"gerrit.instructure.com/ddb-sync/config"

	flag "github.com/spf13/pflag"
)

var ErrExit = flag.ErrHelp

func ParseArgs(args []string) ([]config.OperationPlan, error) {
	flagSet := newFlagSet()

	if len(args) == 0 {
		fmt.Println("ddb-sync:")
		fmt.Println(flagSet.FlagUsages())
		return nil, fmt.Errorf("Improper usage")
	}

	err := flagSet.Parse(args)
	if err != nil {
		// spf13/pflag does weirdness on "-h", "-help", or "--help" and throws a special error and prints usage.
		// We don't want to double message or print a weird error message out.
		if err != ErrExit {
			fmt.Println("ddb-sync:")
			fmt.Println(flagSet.FlagUsages())
		}
		return nil, err
	}

	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("Unknown argument(s): %v", flagSet.Args())
	}

	if file, _ := flagSet.GetString("config-file"); file != "" {
		// Parse the plans from the config file
		plans, err := config.ParseConfigFile(file)
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

	plan := []config.OperationPlan{
		{
			Input: config.Input{
				Region:    inputRegion,
				TableName: inputTable,

				RoleARN: inputRole,
			},
			Output: config.Output{
				Region:    outputRegion,
				TableName: outputTable,

				RoleARN: outputRole,
			},
			Backfill: config.Backfill{
				Disabled: !backfill,
			},
			Stream: config.Stream{
				Disabled: !stream,
			},
		},
	}

	return plan, err
}

func newFlagSet() *flag.FlagSet {
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
