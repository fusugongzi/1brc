package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
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
	f, err := os.Create("./profiles/mem.prof")
	if err != nil {
		fmt.Println("can not create mem.prof" + err.Error())
		return
	}
	if err = pprof.WriteHeapProfile(f); err != nil {
		fmt.Println("WriteHeapProfile error " + err.Error())
		return
	}

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
	go func() {
		// 打开文件
		file, err := os.Open("measurements.txt")
		if err != nil {
			fmt.Println("打开文件错误:", err)
			return
		}

		// 创建带缓冲的读取器，增大缓冲区大小
		reader := bufio.NewReaderSize(file, 500*1024*1024) // 16 KB

		sendBytes := make([]byte, 0)
		leaveBytes := make([]byte, 0)
		// 逐行读取文件
		for {
			readBytes := make([]byte, 64*1024*1024)
			_, err = reader.Read(readBytes)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}
			lastNewLineIdx := bytes.LastIndexByte(readBytes, byte('\n'))
			sendBytes = append(sendBytes, leaveBytes...)
			sendBytes = append(sendBytes, readBytes[:lastNewLineIdx+1]...)
			leaveBytes = make([]byte, len(readBytes[lastNewLineIdx+1:]))
			copy(leaveBytes, readBytes[lastNewLineIdx+1:])
			bytesChan <- sendBytes
		}

		close(bytesChan)

		// 协程处理完数据之后，关闭dataChan
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
	str := string(readBytes)
	strSlice := strings.Split(str, "\n")
	for _, v := range strSlice {
		if v == "" {
			continue
		}
		c := strings.Split(v, ";")[0]
		measure, _ := strconv.ParseFloat(strings.Split(v, ";")[1], 64)
		if exist, ok := m[c]; !ok {
			m[c] = &measurement{
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
	}
	dataChan <- m
}
