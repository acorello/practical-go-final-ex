# Final Exercise - Parallel Downloader

Write a command line program that will download a file over HTTP in parallel.

First, issue a HEAD request to the URL and get the file size from the `Content-Length` HTTP header and the file MD5 signature from the `ETag` HTTP header.

Then create an empty file with the required size and spin `n` goroutines that will download the file in parallel.

Each goroutine will get the URL, destination file name, offset and size to download. It’ll use an [HTTP range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) request to download a chunk and write it to the correct section of the file.

You can use https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet to test your program.

## Possible Extensions

- Test everything
- Add a command line parameter to limit the number of downloading goroutintes
- Add a command line parameter to set the chunk size
- Support retrying of a failed download
  - Add command line parameter to control number of retries
- Add connection timeout
- Cancel all goroutines on error & delete the file

## Hints & Help

### Useful Packages

- `flag` for command line parsing
- `net/http` for HTTP requests
- `os` for working with files

### Creating an Empty File with Given Size

```go
// createEmptyFile creates an empty file in given size
func createEmptyFile(path string, size int64) error {
    file, err := os.Create(path)
    if err != nil {
        return err
    }
    defer file.Close()
    file.Seek(size-1, os.SEEK_SET)
    file.Write([]byte{0})
    return nil
}
```
