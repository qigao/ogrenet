package utils

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
)

func DeCompress(content []byte) ([]byte, error) {
	content = append(content, 0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff)
	fr := flate.NewReader(bytes.NewReader(content))
	content, err := io.ReadAll(fr)
	fmt.Printf("frw:%+v,err:%+v\n", content, err)
	return content, nil
}

func Compress(content []byte, compressLevel int) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	fw, err := flate.NewWriter(buf, compressLevel)
	if err != nil {
		return nil, err
	}
	_, _ = fw.Write(content)
	_ = fw.Flush()
	_ = fw.Close()
	return []byte(buf.String()), nil
}
