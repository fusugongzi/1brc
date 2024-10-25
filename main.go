package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

type measurement struct {
	max int64
	min int64
	sum int64
	cnt int64
	avg int64
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

	originBytesChan := make(chan []byte, 30)

	// 使用协程处理数据
	wg := sync.WaitGroup{}
	wg.Add(runtime.NumCPU() - 1)
	measureChan := make(chan map[string]*measurement, 10)
	for i := 0; i < runtime.NumCPU()-1; i++ {
		go func() {
			processStart := time.Now().UnixMilli()
			for readBytes := range originBytesChan {
				process(readBytes, measureChan)
			}
			fmt.Println("process spend time " + strconv.FormatInt((time.Now().UnixMilli()-processStart), 10))
			wg.Done()
		}()
	}

	fileSize, fileMiddle := findFileMiddle()

	// 读取文件，写入到bytesChan
	var chunkSize int64 = 64 * 1024 * 1024

	fileReadWg := sync.WaitGroup{}
	fileReadWg.Add(2)
	go func(offset, size int64) {
		defer fileReadWg.Done()
		readFile(offset, size, chunkSize, originBytesChan)
	}(0, fileMiddle)

	go func(offset, size int64) {
		defer fileReadWg.Done()
		readFile(offset, size, chunkSize, originBytesChan)
	}(fileMiddle, fileSize-fileMiddle)

	go func() {
		fileReadWg.Wait()

		close(originBytesChan)

		// wait for all chunks to be proccessed before closing the result stream
		wg.Wait()
		close(measureChan)
	}()

	// 汇总最终结果
	result := make(map[string]*measurement)
	for v := range measureChan {
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
			max:  float64(v.max) / float64(10),
			min:  float64(v.min) / float64(10),
			avg:  float64(v.sum) / float64(10) / float64(v.cnt),
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
	var sign bool
	var processDom bool

	for idx, v := range readBytes {
		if v == byte(';') {
			city = unsafeString(unsafe.Pointer(&readBytes[start]), start, idx)
			start = idx + 1
		}
		if v == byte('\n') || idx == len(readBytes)-1 {
			if city != "" {
				sign = true
				processDom = false

				var measure int64 = 0
				for i := start; i < idx; i++ {
					if readBytes[i] == '-' {
						sign = false
					} else if readBytes[i] == '.' {
						processDom = true
					} else if !processDom {
						measure = measure*10 + int64(readBytes[i]-'0')*10
					} else {
						measure = measure + int64(readBytes[i]-'0')
					}
				}
				if !sign {
					measure = 0 - measure
				}

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

func findFileMiddle() (int64, int64) {
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	size := fileInfo.Size()
	middle := size / 2
	_, err = file.Seek(middle, 0)
	if err != nil {
		panic(err)
	}
	buffer := make([]byte, 100)
	for {
		readBytesCnt, err := file.Read(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}
		if readBytesCnt == 0 {
			break
		}
		for _, v := range buffer {
			if v == byte('\n') {
				return size, middle + 1
			} else {
				middle = middle + 1
			}
		}
	}
	return size, middle + 1
}

func readFile(offset, size, chunkSize int64, bytesChan chan []byte) {
	var readSpendTime int64 = 0
	defer func() {
		fmt.Println("read file spend time" + strconv.FormatInt(readSpendTime, 10))
	}()
	file, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	_, err = file.Seek(offset, 0)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, chunkSize)
	leftover := make([]byte, 0, chunkSize)
	var readTotal int64 = 0
	for {
		readStart := time.Now().UnixMilli()
		singleRead, err := file.Read(buf)
		readSpendTime = readSpendTime + (time.Now().UnixMilli() - readStart)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}
		buf = buf[:singleRead]
		readTotal = readTotal + int64(singleRead)
		if readTotal > size {
			idx := int64(singleRead) - (readTotal - size)
			buf = buf[:idx]
		}

		toSend := make([]byte, singleRead)
		copy(toSend, buf)

		lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})

		toSend = append(leftover, buf[:lastNewLineIndex+1]...)
		leftover = make([]byte, len(buf[lastNewLineIndex+1:]))
		copy(leftover, buf[lastNewLineIndex+1:])

		if readTotal >= size {
			toSend = append(toSend, leftover...)
			bytesChan <- toSend
			break
		} else {
			bytesChan <- toSend
		}
	}
}

func unsafeString(ptr unsafe.Pointer, i, j int) string {
	// 计算字符串的长度
	length := j - i

	// 构造字符串头，使用偏移后的指针和长度
	stringHeader := reflect.StringHeader{
		Data: uintptr(ptr),
		Len:  length,
	}

	// 将字符串头转换为字符串
	s := *(*string)(unsafe.Pointer(&stringHeader))

	return s
}
