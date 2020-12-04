package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	benchmark "github.com/dvasilas/proteus-lobsters-bench/internal"
	log "github.com/sirupsen/logrus"
)

func main() {
	var configFile string
	var mergeF1, mergeF2 string
	var threads int
	var load, maxInFlightR, maxInFlightW int64
	flag.StringVar(&configFile, "c", "noArg", "configuration file")
	flag.IntVar(&threads, "t", 1, "number of client threads to be used")
	flag.Int64Var(&load, "l", 0, "target load to be offered")
	flag.Int64Var(&maxInFlightR, "fr", 0, "max read operations in flight")
	flag.Int64Var(&maxInFlightW, "fw", 0, "max write operations in flight")
	preload := flag.Bool("p", false, "preload")
	merge := flag.Bool("m", false, "merge")
	flag.StringVar(&mergeF1, "m1", "noArg", "trace file for merge 1")
	flag.StringVar(&mergeF2, "m2", "noArg", "trace file for merge 2")
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

	if *merge {
		if mergeF1 == "noArg" || mergeF2 == "noArg" {
			flag.Usage()
			return
		}

		f1, err := os.Open(mergeF1)
		if err != nil {
			log.Fatal(err)
		}
		defer f1.Close()

		f2, err := os.Open(mergeF2)
		if err != nil {
			log.Fatal(err)
		}
		defer f2.Close()

		scanner1 := bufio.NewScanner(f1)
		scanner2 := bufio.NewScanner(f2)

		var line string

		if scanner1.Scan() {
			line = scanner1.Text()
		} else {
			log.Fatal(scanner1.Err())
		}

		runTime1, err := strconv.ParseFloat(line, 64)
		if err != nil {
			log.Fatal(err)
		}

		if scanner2.Scan() {
			line = scanner2.Text()
		} else {
			log.Fatal(scanner2.Err())
		}

		runTime2, err := strconv.ParseFloat(line, 64)
		if err != nil {
			log.Fatal(err)
		}

		if scanner1.Scan() {
			line = scanner1.Text()
		} else {
			log.Fatal(scanner1.Err())
		}

		readOps1, err := strconv.ParseFloat(line, 64)
		if err != nil {
			log.Fatal(err)
		}

		if scanner2.Scan() {
			line = scanner2.Text()
		} else {
			log.Fatal(scanner2.Err())
		}

		readOps2, err := strconv.ParseFloat(line, 64)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(runTime1, runTime2)
		fmt.Println(readOps1, readOps2)

		fmt.Println((readOps1 + readOps2) / runTime1)

		return
	}

	if configFile == "noArg" {
		flag.Usage()
		return
	}

	fM, err := os.Create("measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fM.Close()

	fTRead, err := os.Create("readTrace.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fTRead.Close()
	fTWrite, err := os.Create("writeTrace.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer fTWrite.Close()

	bench, err := benchmark.NewBenchmark(configFile, *preload, threads, load, maxInFlightR, maxInFlightW, *dryRun, fM)
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
	err = bench.PrintMeasurements(fM, fTRead, fTWrite)
	if err != nil {
		log.Fatal(err)
	}

}
