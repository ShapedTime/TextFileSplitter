package textFileSplitter

import (
	"bufio"
	"context"
	"fmt"
	"github.com/inhies/go-bytesize"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"os"
	"sync"
)

type config struct {
	File            string
	PoolSize        int
	MemoryFootprint bytesize.ByteSize
}

type textFileSplitter struct {
	config config
}

type chunkProcessor func(lines string, chunkIndex int) interface{}

type chunkMerger func(allResults interface{}, results interface{}) interface{}

var wg sync.WaitGroup

// NewTextFileSplitter takes file as filepath,
// poolsize for number of threads for reading file
// and memoryFootprint for approximate memory usage of the algorithm
func NewTextFileSplitter(file string, poolSize int, memoyFootprint string) (*textFileSplitter, error) {
	memoyFootprintBt, err := bytesize.Parse(memoyFootprint)
	if err != nil {
		return nil, fmt.Errorf("cannot parse memory footprint %s: %v", memoyFootprint, err)
	}

	return &textFileSplitter{
		config: config{
			File:            file,
			PoolSize:        poolSize,
			MemoryFootprint: memoyFootprintBt,
		},
	}, nil
}

// Run splits text files into smaller chunks and calls chunkProcessor using goroutine.
// chunkProcessor results then are sent to mergeChunk over channel so you can merge the chunks of results
// mergeChunk can be nil
func (tfs *textFileSplitter) Run(processChunk chunkProcessor, mergeChunk chunkMerger) (interface{}, error) {
	f, err := os.Open(tfs.config.File)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ch := make(chan interface{})

	wg.Add(1)
	go func() {
		err := processFile(f, tfs.config.PoolSize, uint64(tfs.config.MemoryFootprint),
			func(chunk []byte, i int) {
				str := string(chunk)
				result := processChunk(str, i)
				if mergeChunk != nil {
					ch <- result
				}
			})
		if err != nil {
			log.Printf("error processing file: %v", err)
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(ch)
	}()

	if mergeChunk == nil {
		return nil, nil
	}

	res := func() interface{} {
		var allResults interface{}
		for results := range ch {
			allResults = mergeChunk(allResults, results)
		}
		return allResults
	}()

	wg.Wait()

	return res, nil
}

func processFile(f *os.File, poolSize int, memoryFootprintBytes uint64, processPartition func([]byte, int)) error {
	partitionSize := memoryFootprintBytes / uint64(poolSize)

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, partitionSize)
		return lines
	}}

	r := bufio.NewReader(f)
	sem := semaphore.NewWeighted(int64(poolSize))
	chunkIndex := 0

	for {
		buf := linesPool.Get().([]byte)
		n, err := io.ReadFull(r, buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil && err != io.EOF {
				return err
			}
			break
		}

		nextUntilNewline, err := r.ReadBytes('\n')
		if err == nil {
			buf = append(buf, nextUntilNewline...)
		}

		wg.Add(1)
		err = sem.Acquire(context.Background(), 1)
		if err != nil {
			return fmt.Errorf("cannot acquire semaphore: %v", err)
		}

		chunkIndex++
		go func() {
			tmpIndex := chunkIndex

			processPartition(buf, tmpIndex)
			linesPool.Put(buf)

			wg.Done()
			sem.Release(1)
		}()
	}

	log.Printf("finished processing file")
	return nil
}
