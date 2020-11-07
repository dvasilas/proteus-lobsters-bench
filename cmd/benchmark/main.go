package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	benchmark "github.com/dvasilas/proteus-lobsters-bench/internal"
	log "github.com/sirupsen/logrus"
)

func main() {
	var configFile string
	var threads int
	var load, maxInFlightR, maxInFlightW int64
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.IntVar(&threads, "t", 1, "number of client threads to be used")
	flag.Int64Var(&load, "l", 0, "target load to be offered")
	flag.Int64Var(&maxInFlightR, "fr", 0, "max read operations in flight")
	flag.Int64Var(&maxInFlightW, "fw", 0, "max write operations in flight")
	preload := flag.Bool("p", false, "preload")
	dryRun := flag.Bool("d", false, "dryRun: print configuration and exit")
	test := flag.Bool("test", false, "test: do 1 operation for each op type")

	flag.Usage = func() {
		fmt.Fprintln(os.Stdout, "usage: -c config_file -s system [-p]")
		w := new(tabwriter.Writer)
		w.Init(os.Stdout, 0, 10, 0, '\t', 0)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%v\t%v\n", f.Name, f.Usage)
		})
		w.Flush()
	}

	if len(os.Args) < 2 {
		flag.Usage()
		return
	}

	flag.Parse()

	if configFile == "noArg" {
		flag.Usage()
		return
	}

	bench, err := benchmark.NewBenchmark(configFile, *preload, threads, load, maxInFlightR, maxInFlightW, *dryRun)
	if err != nil {
		log.Fatal(err)
	}

	if *test {
		if err := bench.Test(); err != nil {
			log.Fatal(err)
		}

		return
	}

	if *dryRun {
		return
	}

	if *preload {
		err = bench.Preload()
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	err = bench.Run()
	if err != nil {
		log.Fatal(err)
	}
	bench.PrintMeasurements()
}
