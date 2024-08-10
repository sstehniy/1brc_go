package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

// 0 - min, 1 - max, 2 - mean, 3 - count
type StationData []float64

const BUFFER_SIZE = 64 * 1024 * 1024

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		fmt.Println("could not create CPU profile: ", err)
		return
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Println("could not start CPU profile: ", err)
		return
	}
	defer pprof.StopCPUProfile()

	numCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPUs)

	results := make(chan map[string]StationData, 10000)
	chunks := make(chan []byte)
	wg := sync.WaitGroup{}

	go func() {
		err := readFileInChunks("./data/measurements.txt", chunks)
		if err != nil {
			fmt.Println("Error reading file:", err)
			return
		}
		close(chunks)

		log.Println("file read")
	}()

	for i := 0; i < numCPUs; i++ {
		wg.Add(1)
		go worker(results, chunks, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	finalResult := make(map[string]StationData, 10000)
	for result := range results {
		for station, data := range result {
			if existing, ok := finalResult[station]; !ok {
				finalResult[station] = data
			} else {
				total_count := existing[3] + data[3]
				finalResult[station] = StationData{
					min(existing[0], data[0]),
					max(existing[1], data[1]),
					(existing[2]*existing[3] + data[2]*data[3]) / total_count,
					total_count,
				}
			}
		}
	}

	stations := make([]string, 0, len(finalResult))
	for station := range finalResult {
		stations = append(stations, station)
	}

	// Sort the station names
	sort.Strings(stations)

	outputFile, err := os.Create("results.txt")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()

	stringBuilder := &strings.Builder{}
	// Write final results in alphabetical order
	for _, station := range stations {
		data := finalResult[station]
		stringToWrite := fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", station, data[0], data[2], data[1])
		_, err := stringBuilder.WriteString(stringToWrite)
		if err != nil {
			fmt.Println("Error writing to output file:", err)
			return
		}
	}
	outputFile.WriteString(stringBuilder.String())

	fmt.Println("Results have been written to results.txt")
}

func readFileInChunks(filePath string, chunksChan chan<- []byte) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, BUFFER_SIZE)
	leftover := []byte{}

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}

		chunk := append(leftover, buffer[:n]...)
		lastNewline := bytes.LastIndexByte(chunk, '\n')

		if lastNewline == -1 {
			// No newline found, entire chunk is leftover
			leftover = chunk
		} else {
			// Send complete lines
			chunksChan <- chunk[:lastNewline+1]
			// Keep incomplete line as leftover
			leftover = chunk[lastNewline+1:]
		}

		if err == io.EOF {
			if len(leftover) > 0 {
				chunksChan <- leftover
			}
			return nil
		}
	}
}

func worker(results chan<- map[string]StationData, chunks <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	for chunk := range chunks {
		results <- processChunk(chunk)
	}
}

func processChunk(chunk []byte) map[string]StationData {
	results := make(map[string]StationData)
	start := 0
	for i, b := range chunk {
		if b == '\n' {
			processLine(results, chunk[start:i])
			start = i + 1
		}
	}
	if start < len(chunk) {
		processLine(results, chunk[start:])
	}
	return results
}

func processLine(results map[string]StationData, line []byte) {

	for i, b := range line {
		if b == ';' {
			name := line[:i]
			value := line[i+1:]
			floatVal := parseFloatBytesAlt(value)
			stationName := *(*string)(unsafe.Pointer(&name))
			if data, ok := results[stationName]; !ok {
				results[stationName] = StationData{floatVal, floatVal, floatVal, 1}
			} else {
				data[0] = min(data[0], floatVal)
				data[1] = max(data[1], floatVal)
				data[2] = (data[2]*data[3] + floatVal) / (data[3] + 1)
				data[3]++

				results[stationName] = data
			}
			return
		}
	}

}

// func round(x float64) float64 {
// 	return math.Round(x*10) / 10
// }

func parseFloatBytesAlt(b []byte) float64 {
	sign := float64(1)
	start := 0
	if b[0] == '-' {
		sign = -1
		start = 1
	}

	var result float64

	if len(b)-start == 3 {
		// X.X format
		result = sign * (float64(b[start]-'0') + float64(b[start+2]-'0')*0.1)
	} else {
		// XX.X format
		result = sign * (float64(b[start]-'0')*10 + float64(b[start+1]-'0') + float64(b[start+3]-'0')*0.1)
	}
	return result
}
