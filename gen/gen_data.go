package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var header = []string{
	"orderkey", "partkey", "suppkey", "linenumber", "quantity",
	"extendedprice", "discount", "tax", "returnflag", "linestatus",
	"shipdate", "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment",
}

func randomString(r rune, length int) string {
	return string(r + rune(rand.Intn(length)))
}

func parseSize(sizeStr string) (int64, error) {
	var multiplier int64 = 1
	n := len(sizeStr)
	if n == 0 {
		return 0, fmt.Errorf("empty size")
	}
	last := sizeStr[n-1]
	switch last {
	case 'K', 'k':
		multiplier = 1024
		sizeStr = sizeStr[:n-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		sizeStr = sizeStr[:n-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		sizeStr = sizeStr[:n-1]
	}
	num, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return num * multiplier, nil
}

func main() {
	var output string
	var sizeStr string
	flag.StringVar(&output, "output", "data/lineitem.csv", "output file path, default: data/lineitem.csv")
	flag.StringVar(&sizeStr, "size", "100M", "target file size, e.g. 100M, 1G, default: 100M")
	flag.Parse()

	var targetSize int64
	var err error
	if sizeStr != "" {
		targetSize, err = parseSize(sizeStr)
		if err != nil {
			log.Fatalf("Invalid size: %v", err)
		}
	}

	f, err := os.Create(output)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatalf("Failed to close file: %v", err)
		}
	}()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write(header)
	rand.Seed(time.Now().UnixNano())
	start := time.Now().AddDate(0, 0, -365)
	end := time.Now()

	var writtenRows int
	for {
		row := []string{
			strconv.Itoa(rand.Intn(1000000)),         // orderkey
			strconv.Itoa(rand.Intn(100000)),          // partkey
			strconv.Itoa(rand.Intn(10000)),           // suppkey
			strconv.Itoa(rand.Intn(10)),              // linenumber
			fmt.Sprintf("%.2f", rand.Float64()*50),   // quantity
			fmt.Sprintf("%.2f", rand.Float64()*1000), // extendedprice
			fmt.Sprintf("%.2f", rand.Float64()),      // discount
			fmt.Sprintf("%.2f", rand.Float64()),      // tax
			randomString('A', 3),                     // returnflag
			randomString('F', 2),                     // linestatus
			start.Add(time.Duration(rand.Int63n(int64(end.Sub(start))))).Format("2006-01-02"), // shipdate
			start.Add(time.Duration(rand.Int63n(int64(end.Sub(start))))).Format("2006-01-02"), // commitdate
			start.Add(time.Duration(rand.Int63n(int64(end.Sub(start))))).Format("2006-01-02"), // receiptdate
			"DELIVER IN PERSON", // shipinstruct
			"TRUCK",             // shipmode
			"No comment",        // comment
		}
		w.Write(row)
		writtenRows++
		if writtenRows%1000 == 0 {
			w.Flush()
		}
		// 判断是否达到目标
		if targetSize > 0 {
			fi, _ := f.Stat()
			if fi.Size() >= targetSize {
				break
			}
		}
	}
	fmt.Printf("Generated %d rows to %s (%.2f MB)\n", writtenRows, output, float64(targetSize)/1024/1024)
}
