package dotc1z

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func fileCRC32CHex(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", 0, err
	}

	h := crc32.New(crc32cTable)
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, err
	}

	return fmt.Sprintf("%08x", h.Sum32()), stat.Size(), nil
}
