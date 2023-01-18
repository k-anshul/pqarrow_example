package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/gcsblob"
)

type ParquetReader struct {
	ctx      context.Context
	bucket   *blob.Bucket
	offset   int64
	fileName string
	size     int64
	call     int
}

func (f *ParquetReader) ReadAt(p []byte, off int64) (n int, err error) {
	fmt.Printf("reading %v bytes at offset %v\n", len(p), off)

	reader, err := f.bucket.NewRangeReader(f.ctx, f.fileName, off, int64(len(p)), nil)

	// reader, err := f.bucket.NewReader(f.ctx, f.fileName, nil)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	// buf := make([]byte, len(p))
	read, readErr := reader.Read(p)
	if readErr != nil {
		return n, readErr
	}

	f.offset += int64(read)
	f.call += 1
	return read, nil
}

func (f *ParquetReader) Size() int64 {
	return f.size
}

func (f *ParquetReader) Close() error {
	fmt.Printf("made %v calls\n", f.call)
	return f.bucket.Close()
}

func (f *ParquetReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.offset + offset
	case io.SeekEnd:
		abs = f.size + offset
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("bytes.Reader.Seek: negative position")
	}
	f.offset = abs

	return abs, nil
}

func NewParquetReader(file string) *ParquetReader {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "gs://druid-demo.gorill-stage.io")
	if err != nil {
		panic(err)
	}

	list := bucket.List(&blob.ListOptions{Prefix: file})
	for {
		obj, err := list.Next(ctx)

		if err != nil {
			panic(err)
		}
		if strings.Contains(obj.Key, file) {
			return &ParquetReader{ctx: ctx, bucket: bucket, offset: 0, fileName: obj.Key, size: obj.Size}
		}
	}
}

func panicIfError(err error) {
	if err != nil {
		fmt.Printf("err %v\n", err)
		panic(err)
	}
}

func main() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	parquetReader := NewParquetReader("safegraph/2020/01/green_tripdata_2020-01.parquet")
	// arrow parquet reader will close parquetReader
	// defer handler.Close()

	props := parquet.NewReaderProperties(mem)
	props.BufferedStreamEnabled = true
	props.BufferSize = 1024 * 1024

	pf, err := file.NewParquetReader(parquetReader, file.WithReadProps(props))
	panicIfError(err)
	defer pf.Close()

	// not sure what is optimum BatchSize
	// disabling parallel since parquetReader is not thread safe as of now
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1000, Parallel: false}, mem)
	panicIfError(err)

	numRowGroups := pf.NumRowGroups()
	if numRowGroups == 0 {
		panicIfError(fmt.Errorf("invalid parquet"))
	}

	cols := pf.RowGroup(0).NumColumns()
	colIndices := make([]int, cols)
	for i := 0; i < cols; i++ {
		colIndices[i] = i
	}

	r, err := reader.GetRecordReader(parquetReader.ctx, colIndices, nil)
	panicIfError(err)

	records := make([]arrow.Record, 1)
	for i := 0; i < 1; i++ {
		rec, err := r.Read()
		panicIfError(err)

		rec.Retain()
		records[i] = rec
	}
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	schema, err := reader.Schema()
	panicIfError(err)

	table := array.NewTableFromRecords(schema, records)
	defer table.Release()

	println(table.NumRows())
	file, err := os.Create("out.parquet")
	defer file.Close()
	panicIfError(err)

	panicIfError(pqarrow.WriteTable(table, file, table.NumRows(), nil, pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem))))

}
