package vector

import (
	"math"
	"math/rand"
)

type DistanceMethod int

const (
	L2_DISTANCE DistanceMethod = iota
	COSINE_DISTANCE
	INNERPRODUCT_DISTANCE
)

func (m DistanceMethod) getFunc() func(T, T) (float64, error) {
	switch m {
	case L2_DISTANCE:
		return L2Distance
	case COSINE_DISTANCE:
		return CosDistance
	case INNERPRODUCT_DISTANCE:
		return InnerProduct
	}
	return nil
}

// initCenters initializes centroids using kmeans++ algorithm.
// https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf
func initCenters(samples []T, nCenters int, distanceMethod DistanceMethod) (centers []T, lowerBound []float32, err error) {
	distanceFunc := distanceMethod.getFunc()

	centers = make([]T, 0, nCenters)
	lowerBound = make([]float32, len(samples)*nCenters)

	centers = append(centers, samples[rand.Intn(len(samples))])

	weight := make([]float32, len(samples))
	for j := range samples {
		weight[j] = math.MaxFloat32
	}

	for i := range centers {
		sum := 0.0

		for j := range samples {
			d64, err := distanceFunc(samples[j], centers[i])
			if err != nil {
				return nil, nil, err
			}
			distance := float32(d64)

			// Set lower bound
			lowerBound[j*nCenters+i] = distance

			// Use distance squared for weighted probability distribution
			distance *= distance

			if distance < weight[j] {
				weight[j] = distance
			}

			sum += float64(weight[j])
		}

		// Only compute lower bound on last iteration
		if i+1 == nCenters {
			break
		}

		// Choose new center using weighted probability distribution.
		choice := sum * rand.Float64()
		var j = 0
		for ; j < len(samples); j++ {
			choice -= float64(weight[j])
			if choice <= 0 {
				break
			}
		}

		centers = append(centers, samples[j])
	}

	return centers, lowerBound, err
}

// ElkanKmeans performs k-means clustering using the Elkan algorithm, which
// uses the triangle inequality to reduce comparisons.
// https://www.aaai.org/Papers/ICML/2003/ICML03-022.pdf
func ElkanKmeans(samples []T, nCenters int, distanceMethod DistanceMethod) (centers []T, err error) {
	var lowerBound []float32
	centers, lowerBound, err = initCenters(samples, nCenters, distanceMethod)
	if err != nil {
		return nil, err
	}
	dimensions := len(centers[0])
	numCenters := len(centers)

	newCenters := make([]T, numCenters)
	centerCounts := make([]int, numCenters)
	closestCenters := make([]int, len(samples))
	upperBound := make([]float32, len(samples))
	s := make([]float32, numCenters)
	halfcdist := make([]float32, numCenters*numCenters)
	newcdist := make([]float32, numCenters)

	distanceFunc := distanceMethod.getFunc()

	// Give 500 iterations to converge.
	for iteration := 0; iteration < 500; iteration++ {
		changes := 0

		// For all centers, compute distance.
		for j := range centers {
			for k := j + 1; k < numCenters; k++ {
				f, err := distanceFunc(centers[j], centers[k])
				if err != nil {
					return nil, err
				}
				distance := float32(0.5 * f)
				halfcdist[j*numCenters+k] = distance
				halfcdist[k*numCenters+j] = distance
			}
		}

		// For all centers c, compute s(c).
		for j := range centers {
			var minDistance float32 = math.MaxFloat32
			for k := range centers {
				if j == k {
					continue
				}
				distance := halfcdist[j*numCenters+k]
				if distance < minDistance {
					minDistance = distance
				}
			}
			s[j] = minDistance
		}

		rjreset := iteration != 0

		for j := range samples {
			// Step 2: Identify all points x such that u(x) <= s(c(x)).
			if upperBound[j] <= s[closestCenters[j]] {
				continue
			}

			rj := rjreset
			for k := range centers {
				// Step 3: For all remaining points x and centers c.
				if k == closestCenters[j] {
					continue
				}

				if upperBound[j] <= lowerBound[j*numCenters+k] {
					continue
				}

				if upperBound[j] <= halfcdist[closestCenters[j]*numCenters+k] {
					continue
				}

				dxcx := upperBound[j]
				vec := samples[j]
				// Step 3a.
				if rj {
					f, err := distanceFunc(vec, centers[closestCenters[j]])
					if err != nil {
						return nil, err
					}
					dxcx = float32(f)
					// d(x,c(x)) computed, which is a form of d(x,c).
					lowerBound[j*numCenters+closestCenters[j]] = dxcx
					upperBound[j] = dxcx
					rj = false
				}

				// Step 3b.
				if dxcx > lowerBound[j*numCenters+k] || dxcx > halfcdist[closestCenters[j]*numCenters+k] {
					f, err := distanceFunc(vec, centers[closestCenters[j]])
					if err != nil {
						return nil, err
					}
					dxc := float32(f)
					// d(x,c) calculated.
					lowerBound[j*numCenters+k] = dxc

					if dxc < dxcx {
						closestCenters[j] = k
						// c(x) changed.
						upperBound[j] = dxc
						changes++
					}
				}
			}
		}

		// Step 4: For each center c, let m(c) be the mean of all points assigned.
		for j := range newCenters {
			newCenters[j] = make(T, dimensions)
			centerCounts[j] = 0
		}

		for j := range samples {
			vec := samples[j]
			closestCenter := closestCenters[j]
			// Increment sum and count of closest center.
			newCenter := newCenters[closestCenter]
			for k := 0; k < dimensions; k++ {
				newCenter[k] += vec[k]
			}
			centerCounts[closestCenter]++
		}

		for j := range centerCounts {
			vec := newCenters[j]
			if centerCounts[j] > 0 {
				// Double avoids overflow, but requires more memory.
				for k := 0; k < dimensions; k++ {
					if math.IsInf(float64(vec[k]), 0) {
						if vec[k] > 0 {
							vec[k] = math.MaxFloat32
						} else {
							vec[k] = -math.MaxFloat32
						}
					}
					vec[k] /= float32(centerCounts[j])
				}
			} else {
				// This is a bug in the original code.
				// TODO: Handle empty centers properly.
				for k := 0; k < dimensions; k++ {
					newCenters[j][k] = rand.Float32()
				}
			}
			// Normalize if needed.
			if distanceMethod == COSINE_DISTANCE {
				applyNorm(vec)
			}
		}

		// Step 5.
		for j := 0; j < numCenters; j++ {
			f, err := distanceFunc(centers[j], newCenters[j])
			if err != nil {
				return nil, err
			}
			newcdist[j] = float32(f)
		}

		for j := range samples {
			for k := range centers {
				distance := lowerBound[j*numCenters+k] - newcdist[k]
				if distance < 0 {
					distance = 0
				}
				lowerBound[j*numCenters+k] = distance
			}
		}

		// Step 6.
		// We reset r(x) before Step 3 in the next iteration.
		for j := range samples {
			upperBound[j] += newcdist[closestCenters[j]]
		}

		// Step 7.
		for j := 0; j < numCenters; j++ {
			centers[j] = newCenters[j]
		}

		if changes == 0 && iteration != 0 {
			break
		}
	}
	return centers, nil
}

// applyNorm applies the vectors norm.
func applyNorm(vec T) {
	norm := Norm(vec)
	// TODO: Handle zero norm.
	if norm > 0 {
		for i := range vec {
			vec[i] /= float32(norm)
		}
	}
}
