package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type city struct {
	name string
	m    float64
}

type measurement struct {
	name string
	max  float64
	min  float64
	sum  float64
	cnt  int64
	avg  float64
}

func main() {
	start := time.Now().UnixMilli()

	readFileStart := time.Now().UnixMilli()
	bytes, err := os.ReadFile("measurements.txt")
	if err != nil {
		fmt.Println("read file error")
	}
	fmt.Println("read file spend: " + strconv.FormatInt(time.Now().UnixMilli()-readFileStart, 10) + " ms")

	cities := make([]city, 0)
	analyStart := time.Now().UnixMilli()
	for _, v := range strings.Split(string(bytes), "\n") {
		vs := strings.Split(v, ";")
		m, _ := strconv.ParseFloat(vs[1], 64)
		cities = append(cities, city{
			name: vs[0],
			m:    m,
		})
	}
	fmt.Println("analy spend: " + strconv.FormatInt(time.Now().UnixMilli()-analyStart, 10) + " ms")

	calStart := time.Now().UnixMilli()
	measurementMap := make(map[string]*measurement)
	for _, v := range cities {
		if exist, ok := measurementMap[v.name]; !ok {
			measurementMap[v.name] = &measurement{
				name: v.name,
				min:  v.m,
				max:  v.m,
				sum:  v.m,
				cnt:  1,
			}
		} else {
			if v.m < exist.min {
				exist.min = v.m
			} else if v.m > exist.max {
				exist.max = v.m
			}
			exist.sum = exist.sum + v.m
			exist.cnt++
		}
	}
	fmt.Println("cal spend: " + strconv.FormatInt(time.Now().UnixMilli()-calStart, 10) + " ms")

	measurements := make([]*measurement, 0)
	for _, v := range measurementMap {
		v := v
		v.avg = v.sum / float64(v.cnt)
		measurements = append(measurements, v)
	}
	sort.SliceStable(measurements, func(i, j int) bool {
		return measurements[i].name < measurements[j].name
	})
	for _, v := range measurements {
		fmt.Println(fmt.Sprintf("%s/%.2f/%.2f/%.2f", v.name, v.min, v.avg, v.max))
	}

	fmt.Println("total spend: " + strconv.FormatInt(time.Now().UnixMilli()-start, 10) + " ms")
}
