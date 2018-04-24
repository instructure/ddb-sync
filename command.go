package main

import (
	"fmt"

	"gerrit.instructure.com/ddb-sync/plan"

	flag "github.com/spf13/pflag"
)

func ParseArgs(args []string) ([]plan.Plan, error) {
	flag := flagSet()
	err := flag.Parse(args)
	if err != nil {
		return nil, err
	}

	if flag.NArg() > 0 {
		return nil, fmt.Errorf("Unknown argument(s): %v", flag.Args())
	}

	inputRegion, _ := flag.GetString("input-region")
	inputTable, _ := flag.GetString("input-table")
	inputRole, _ := flag.GetString("input-role-arn")

	outputRegion, _ := flag.GetString("output-region")
	outputTable, _ := flag.GetString("output-table")
	outputRole, _ := flag.GetString("output-role-arn")

	backfill, _ := flag.GetBool("backfill")
	stream, _ := flag.GetBool("stream")

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
