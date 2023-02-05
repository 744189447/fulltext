package fulltext

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"os"
	"strings"
)

func readLines(file string) ([]string, error) {
	return readSplitter(file, '\n')
}

func readSplitter(file string, splitter byte) (lines []string, err error) {
	fin, err := os.Open(file)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		return
	}

	r := bufio.NewReader(fin)
	for {
		line, err := r.ReadString(splitter)
		if err == io.EOF {
			break
		}
		line = strings.Replace(line, string(splitter), "", -1)
		lines = append(lines, line)
	}
	return
}

//func float32ToByte(float float32) []byte {
//	bits := math.Float32bits(float)
//	b := make([]byte, 4)
//	binary.LittleEndian.PutUint32(b, bits)
//	return b
//}
//
//func byteToFloat32(b []byte) float32 {
//	if len(b) == 0 {
//		return 0
//	}
//	bits := binary.LittleEndian.Uint32(b)
//	return math.Float32frombits(bits)
//}
//

func uint64ToByte(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func byteToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func uint32ToByte(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func byteToUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func anyToByte(m any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func byteToAny(b []byte, a any) error {
	buf := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(a)
}
