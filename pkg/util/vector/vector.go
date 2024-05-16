package vector

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/vector/vectorpb"
	"github.com/cockroachdb/errors"
)

// MaxDim is the maximum number of dimensions a vector can have.
const MaxDim = 16000

// T is the type of a PGVector-like vector.
type T []float32

// ParseVector parses the Postgres string representation of a vector.
func ParseVector(input string) (T, error) {
	input = strings.TrimSpace(input)
	if !strings.HasPrefix(input, "[") || !strings.HasSuffix(input, "]") {
		return T{}, pgerror.New(pgcode.InvalidTextRepresentation,
			"malformed vector literal: Vector contents must start with \"[\" and"+
				" end with \"]\"")
	}

	input = strings.TrimPrefix(input, "[")
	input = strings.TrimSuffix(input, "]")
	parts := strings.Split(input, ",")

	if len(parts) > MaxDim {
		return T{}, pgerror.Newf(pgcode.ProgramLimitExceeded, "vector cannot have more than %d dimensions", MaxDim)
	}

	vector := make([]float32, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return T{}, pgerror.New(pgcode.InvalidTextRepresentation, "invalid input syntax for type vector: empty string")
		}

		val, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return T{}, pgerror.New(pgcode.InvalidTextRepresentation, "invalid input syntax for type vector: "+part)
		}

		vector[i] = float32(val)
	}

	return vector, nil
}

// String implements the fmt.Stringer interface.
func (v T) String() string {
	strs := make([]string, len(v))
	for i, v := range v {
		strs[i] = fmt.Sprintf("%g", v)
	}
	return "[" + strings.Join(strs, ",") + "]"
}

// Size returns the size of the vector in bytes.
func (v T) Size() uintptr {
	return uintptr(len(v)) * 4
}

// Compare returns -1 if v < v2, 1 if v > v2, and 0 if v == v2.
func (v T) Compare(v2 T) (int, error) {
	if err := checkDims(v, v2); err != nil {
		return 0, err
	}
	for i := range v {
		if v[i] < v2[i] {
			return -1, nil
		} else if v[i] > v2[i] {
			return 1, nil
		}
	}
	return 0, nil
}

// Encode encodes the vector as a byte array suitable for storing in KV.
func Encode(appendTo []byte, t T) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(t)))
	for i := range t {
		appendTo = encoding.EncodeUntaggedFloat32Value(appendTo, t[i])
	}
	return appendTo, nil
}

// Decode decodes the byte array into a vector.
func Decode(b []byte) (ret T, err error) {
	var n uint32
	b, n, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}
	ret = make(T, n)
	for i := range ret {
		b, ret[i], err = encoding.DecodeUntaggedFloat32Value(b)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func checkDims(t T, t2 T) error {
	if len(t) != len(t2) {
		return pgerror.Newf(pgcode.DataException, "different vector dimensions %d and %d", len(t), len(t2))
	}
	return nil
}

// L1Distance returns the L1 (Manhattan) distance between t and t2.
func L1Distance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	var distance float32
	for i := range len(t) {
		diff := t[i] - t2[i]
		distance += float32(math.Abs(float64(diff)))
	}
	return float64(distance), nil
}

// L2Distance returns the Euclidean distance between t and t2.
func L2Distance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	var distance float32
	for i := range len(t) {
		diff := t[i] - t2[i]
		distance += diff * diff
	}
	return math.Sqrt(float64(distance)), nil
}

// CosDistance returns the cosine distance between t and t2.
func CosDistance(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	var distance, normA, normB float32
	for i := range len(t) {
		distance += t[i] * t2[i]
		normA += t[i] * t[i]
		normB += t2[i] * t2[i]
	}
	// Use sqrt(a * b) over sqrt(a) * sqrt(b)
	similarity := float64(distance) / math.Sqrt(float64(normA)*float64(normB))
	/* Keep in range */
	if similarity > 1 {
		similarity = 1
	} else if similarity < -1 {
		similarity = -1
	}
	return 1 - similarity, nil
}

// InnerProduct returns the negative inner product of t1 and t2.
func InnerProduct(t T, t2 T) (float64, error) {
	if err := checkDims(t, t2); err != nil {
		return 0, err
	}
	var distance float32
	for i := range len(t) {
		distance += t[i] * t2[i]
	}
	return float64(distance), nil
}

// NegInnerProduct returns the negative inner product of t1 and t2.
func NegInnerProduct(t T, t2 T) (float64, error) {
	p, err := InnerProduct(t, t2)
	return p * -1, err
}

// Norm returns the L2 norm of t.
func Norm(t T) float64 {
	var norm float64
	for i := range t {
		norm += float64(t[i]) * float64(t[i])
	}
	return math.Sqrt(norm)
}

// Add returns t+t2, pointwise.
func Add(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] + t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
	}
	return ret, nil
}

// Minus returns t-t2, pointwise.
func Minus(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] - t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
	}
	return ret, nil
}

// Mult returns t*t2, pointwise.
func Mult(t T, t2 T) (T, error) {
	if err := checkDims(t, t2); err != nil {
		return nil, err
	}
	ret := make(T, len(t))
	for i := range t {
		ret[i] = t[i] * t2[i]
	}
	for i := range ret {
		if math.IsInf(float64(ret[i]), 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: overflow")
		}
		if ret[i] == 0 && !(t[i] == 0 || t2[i] == 0) {
			return nil, pgerror.New(pgcode.NumericValueOutOfRange, "value out of range: underflow")
		}
	}
	return ret, nil
}

func distanceFuncFromProto(distanceMethod vectorpb.DistanceFunction) func(T, T) (float64, error) {
	switch distanceMethod {
	case vectorpb.DistanceFunction_L2:
		return L2Distance
	case vectorpb.DistanceFunction_IP:
		return InnerProduct
	case vectorpb.DistanceFunction_COSINE:
		return CosDistance
	}
	panic(fmt.Sprintf("unsupported distance function %s", distanceMethod))
}

// GetClosestCentroid returns the centroid from the index configuration that is
// closest to the given vector t.
func GetClosestCentroid(t T, cfg vectorpb.Config) (T, error) {
	switch cfg.IndexType.(type) {
	case *vectorpb.Config_IvfFlat:
	default:
		return nil, errors.AssertionFailedf("unsupported index type %T", cfg.IndexType)
	}
	distanceFunc := distanceFuncFromProto(cfg.DistanceFunction)

	ivf := cfg.GetIvfFlat()
	if len(ivf.Centroids) == 0 {
		return nil, errors.AssertionFailedf("no centroids found in index configuration")
	}
	var closest T
	closestDistance := math.MaxFloat64
	for _, centroid := range ivf.Centroids {
		dist, err := distanceFunc(t, centroid.Centroid)
		if err != nil {
			return nil, err
		}
		if dist < closestDistance {
			closest = centroid.Centroid
			closestDistance = dist
		}
	}
	return closest, nil
}

// Random returns a random vector.
func Random(rng *rand.Rand) T {
	n := 1 + rng.Intn(1000)
	v := make(T, n)
	for i := range v {
		v[i] = math.Float32frombits(rng.Uint32())
	}
	return v
}
