package tdigest

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
)

const magic uint32 = 0x867CA8A3 // "DGST"
const version1 uint32 = 1
const maxNumCentroids = 128 * 1024 * 1024

func serializedSize(numCentroids int) int {
	if numCentroids < 0 || numCentroids > maxNumCentroids {
		panic(fmt.Sprintf("invalid number of centroids %d", numCentroids))
	}
	size := 0
	size += 4                      // magic
	size += 4                      // version
	size += 4                      // delta
	size += 4                      // number of centroids
	size += 8                      // min
	size += 8                      // max
	size += numCentroids * (8 + 8) // each centroid: mean, weight
	return size
}

func (t *TDigest) SerializedSize() int {
	return serializedSize(len(t.centroids))
}

func (t *TDigest) Serialize(appendTo []byte) []byte {
	appendTo = slices.Grow(appendTo, t.SerializedSize())
	appendTo = binary.LittleEndian.AppendUint32(appendTo, magic)
	appendTo = binary.LittleEndian.AppendUint32(appendTo, version1)
	appendTo = binary.LittleEndian.AppendUint32(appendTo, uint32(t.delta))
	appendTo = binary.LittleEndian.AppendUint32(appendTo, uint32(len(t.centroids)))
	appendTo = binary.LittleEndian.AppendUint64(appendTo, math.Float64bits(t.min))
	appendTo = binary.LittleEndian.AppendUint64(appendTo, math.Float64bits(t.max))
	for _, c := range t.centroids {
		appendTo = binary.LittleEndian.AppendUint64(appendTo, math.Float64bits(c.Mean))
		appendTo = binary.LittleEndian.AppendUint64(appendTo, math.Float64bits(c.Weight))
	}
	return appendTo
}

// Deserialize deserializes a t-digest from buf into dest.
//
// The centroid slice in dest is reused.
//
// Returns the number of bytes read from buf and any error encountered.
func Deserialize(dest *TDigest, buf []byte) (bufOffset int, _ error) {
	if len(buf) < serializedSize(0) {
		return 0, fmt.Errorf("buffer too small to be a valid t-digest")
	}
	if m := binary.LittleEndian.Uint32(buf); m != magic {
		return 0, fmt.Errorf("invalid t-digest magic %X", m)
	}
	if v := binary.LittleEndian.Uint32(buf[4:]); v != version1 {
		return 0, fmt.Errorf("unknown t-digest version %d", v)
	}
	dest.delta = int(binary.LittleEndian.Uint32(buf[8:]))
	if dest.delta < 0 {
		return 0, fmt.Errorf("invalid delta %d", dest.delta)
	}
	n := binary.LittleEndian.Uint32(buf[12:])
	if n > maxNumCentroids {
		panic(fmt.Sprintf("invalid number of centroids %d", n))
	}
	bufOffset = serializedSize(int(n))
	if len(buf) < bufOffset {
		return 0, fmt.Errorf("buffer too small to be a valid t-digest")
	}
	dest.min = math.Float64frombits(binary.LittleEndian.Uint64(buf[16:]))
	dest.max = math.Float64frombits(binary.LittleEndian.Uint64(buf[24:]))
	dest.totalWeight = 0
	buf = buf[32:]
	dest.centroids = slices.Grow(dest.centroids[:0], int(n))[:n]
	for i := range dest.centroids {
		m := math.Float64frombits(binary.LittleEndian.Uint64(buf[i*16:]))
		w := math.Float64frombits(binary.LittleEndian.Uint64(buf[i*16+8:]))
		if math.IsNaN(m) || math.IsNaN(w) || w < 0 || (i > 0 && m < dest.centroids[i-1].Mean) {
			return 0, fmt.Errorf("invalid centroid data")
		}
		dest.centroids[i] = Centroid{Mean: m, Weight: w}
		dest.totalWeight += w
	}
	return bufOffset, nil
}
