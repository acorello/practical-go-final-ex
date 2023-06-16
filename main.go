package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
)

type RequestId uint64

type DownloadRequest struct {
	RequestId
	URL            string
	ResourceOffset int64
	Buffer         []byte
}

type DownloadReport struct {
	RequestId
	Err error
}

func main() {
	const SHA1SUM = "52acba6856142ca528e4617e06fa2f5f0212ce86"
	const URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet"
	// Get head of resource
	//		header.content-length
	//		header.etag
	res, err := http.Head(URL)
	if err != nil {
		log.Fatalf("HTTP HEAD failed for %q", URL)
	}
	contentLength, err := strconv.ParseInt(res.Header.Get("content-length"), 10, 64)
	if err != nil {
		log.Fatalf("Content-Length not present in %q", URL)
	}
	checksum := res.Header.Get("etag")
	if checksum == "" {
		log.Fatalf("etag not present in %q", URL)
	}
	// The returned value doesn't seem to match.
	checksum = SHA1SUM

	fmt.Printf("Length %d\nChecks %s\n", contentLength, checksum)
	// Create a file of ‹content-length› size
	tempFile, err := os.CreateTemp("", "practical-go-final-ex_")
	if err != nil {
		log.Fatalf("failed to create temporary file: %s", err)
	}
	defer tempFile.Close()
	GrowFile(tempFile, contentLength)

	downloadRequests := make(chan DownloadRequest)
	downloadReports := make(chan DownloadReport)

	const WORKERS_COUNT = 4

	for w := WORKERS_COUNT; w > 0; w-- {
		go download(downloadRequests, downloadReports)
	}

	//	VERSION 1:
	//	CONCURRENT PROCESSES:
	// 		N workers receive on 1 channel and write response on 1 channel
	//		1 CONTROLLER:
	//			- splits work and feeds channel
	//			- keeps track of number of chunks in the workd
	//			- reads responses and decides whether to abort, retry, accept
	//			- when all jobs are completed and there is no more work finalize work
	const CHUNK_LENGTH int64 = 4 << 20

	//	How to calculate offsets?
	//	how many offsets? totalSize / CHUNK_SIZE; but integer division will truncate down
	var offset int64
	// var submittedJobs uint64
	// I increment offset by CHUNKSIZE until it reaches the end of the file
	for offset = 0; offset < contentLength; offset += CHUNK_LENGTH {
		bufferLength := CHUNK_LENGTH
		if offset+CHUNK_LENGTH > contentLength {
			bufferLength = contentLength - offset
		}
		buffer := make([]byte, 0, bufferLength)

		req := DownloadRequest{
			RequestId:      RequestId(offset),
			URL:            URL,
			ResourceOffset: offset,
			Buffer:         buffer,
		}
		select {
		case downloadRequests <- req:
			// keep feeding until the channel is full
			continue

		}
	}

	// A number of chunks and associated buffers possibly on a memory-mapped file
	// A number of goroutines listening on a channel

	// Spin off N goroutines receiving a ‹DownloadRequest› on the input channel:
	//
	//		DownloadRequest { URL string, offset uint, buffer []byte }
	//
	// Each coroutine will signal completion returning the ‹DownloadReport› on another channel.
	//
	//		DownloadReport { URL string, offset uint, err error }
	//
	// I'm assuming that the algorithm will feed non-overlapping buffers of the file so the offset can be used to identify each segment being downloaded.
	//
	// Finally calculate the checksum and if it passes return success.

	// We need to be able to associate requests and responses.

}

func download(reqs <-chan DownloadRequest, res chan<- DownloadReport) {
	for r := range reqs {
		fmt.Println("Received %#v", r)
		res <- DownloadReport{
			RequestId: r.RequestId,
			Err:       nil,
		}
	}
}

func createEmptyFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Seek(size-1, io.SeekEnd)
	file.Write([]byte{0})
	return nil
}

func GrowFile(file *os.File, size int64) {
	file.Seek(size-1, io.SeekEnd)
	file.Write([]byte{0})
}
