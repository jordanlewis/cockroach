package exec

// Bitmap is a simple bitmap structure implemented on top of a byte slice.
type Bitmap []byte

// Get returns true if the bit at position i is set and false otherwise.
func (b Bitmap) Get(i int) bool {
	return (b[i/8] & (1 << uint(i%8))) != 0
}

// set sets the bit at position i if v is true and clears the bit at position i
// otherwise.
func (b Bitmap) set(i int, v bool) Bitmap {
	j := i / 8
	for len(b) <= j {
		b = append(b, 0)
	}
	if v {
		b[j] |= 1 << uint(i%8)
	} else {
		b[j] &^= 1 << uint(i%8)
	}
	return b
}
