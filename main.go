package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"
)

type measurement struct {
	max float64
	min float64
	sum float64
	cnt int64
	avg float64
}

type computedResult struct {
	name string
	max  float64
	min  float64
	avg  float64
}

func main() {
	go func() {
		http.HandleFunc("/prof", startProfileHandler)
		http.ListenAndServe(":9092", nil)
	}()

	start := time.Now().UnixMilli()

	bytesChan := make(chan []byte, 15)

	// 使用协程处理数据
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU() - 1)
	dataChan := make(chan map[string]*measurement, 10)
	for i := 0; i < runtime.NumCPU()-1; i++ {
		go func() {
			for readBytes := range bytesChan {
				process(readBytes, dataChan)
			}
			wg.Done()
		}()
	}

	// 读取文件，写入到bytesChan
	chunkSize := 64 * 1024 * 1024
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	go func() {
		buf := make([]byte, chunkSize)
		leftover := make([]byte, 0, chunkSize)
		for {
			readTotal, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}
			buf = buf[:readTotal]

			toSend := make([]byte, readTotal)
			copy(toSend, buf)

			lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})

			toSend = append(leftover, buf[:lastNewLineIndex+1]...)
			leftover = make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(leftover, buf[lastNewLineIndex+1:])

			bytesChan <- toSend

		}
		close(bytesChan)

		// wait for all chunks to be proccessed before closing the result stream
		wg.Wait()
		close(dataChan)
	}()

	// 汇总最终结果
	result := make(map[string]*measurement)
	for v := range dataChan {
		for c, measure := range v {
			c := c
			measure := measure
			if result[c] == nil {
				result[c] = measure
			} else {
				exist := result[c]
				if measure.min < exist.min {
					exist.min = measure.min
				}
				if measure.max > exist.max {
					exist.max = measure.max
				}
				exist.sum = exist.sum + measure.sum
				exist.cnt = exist.cnt + measure.cnt
			}
		}
	}

	// 输出最终结果
	computedResults := make([]*computedResult, 0)
	for k, v := range result {
		computedResults = append(computedResults, &computedResult{
			name: k,
			max:  v.max,
			min:  v.min,
			avg:  float64(v.sum) / float64(v.cnt),
		})
	}
	sort.SliceStable(computedResults, func(i, j int) bool {
		return computedResults[i].name < computedResults[j].name
	})
	for _, v := range computedResults {
		fmt.Println(fmt.Sprintf("%s/%.2f/%.2f/%.2f", v.name, v.min, v.avg, v.max))
	}

	fmt.Println("spend time: " + strconv.FormatInt(time.Now().UnixMilli()-start, 10) + " ms")
}

func process(readBytes []byte, dataChan chan map[string]*measurement) {
	m := make(map[string]*measurement)
	start := 0
	var city string

	for idx, v := range readBytes {
		if v == byte(';') {
			city = string(readBytes[start:idx])
			start = idx + 1
		}
		if v == byte('\n') {
			if city != "" {
				measure, _ := strconv.ParseFloat(string(readBytes[start:idx]), 64)
				if exist, ok := m[city]; !ok {
					m[city] = &measurement{
						min: measure,
						max: measure,
						sum: measure,
						cnt: 1,
					}
				} else {
					if measure < exist.min {
						exist.min = measure
					} else if measure > exist.max {
						exist.max = measure
					}
					exist.sum = exist.sum + measure
					exist.cnt++
				}
				city = ""
			}
			start = idx + 1
		}
	}

	dataChan <- m
}

// startProfileHandler 启动 CPU profiling
func startProfileHandler(w http.ResponseWriter, r *http.Request) {
	// 创建 profile 文件
	f, err := os.Create("mem.prof")
	if err != nil {
		return
	}

	// 启动 CPU profiling
	if err = pprof.WriteHeapProfile(f); err != nil {
		return
	}

	w.Write([]byte("Memory profiling completed, profile saved as mem.prof"))
}
