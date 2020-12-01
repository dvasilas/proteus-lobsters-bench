package distributions

import (
	"math"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	returnCode := m.Run()
	os.Exit(returnCode)
}

var tests = [][]struct {
	Bin   int64
	Count int64
}{
	[]struct {
		Bin   int64
		Count int64
	}{
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   0,
			Count: 4000,
		},
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   10,
			Count: 500,
		},
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   20,
			Count: 200,
		},
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   30,
			Count: 1000,
		},
	},
	[]struct {
		Bin   int64
		Count int64
	}{
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   0,
			Count: 995,
		},
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   10,
			Count: 0,
		},
		struct {
			Bin   int64
			Count int64
		}{
			Bin:   500,
			Count: 5,
		},
	},
}

func TestSample(t *testing.T) {
	for _, histVotes := range tests {
		var histNElements, histNVotes, sampleNElements int64

		// compute the number of elements in the input histogram
		for _, bin := range histVotes {
			histNElements += bin.Count
		}

		// compute the number of votes it gets to reproduce the input histogram
		// simplify all bins (except the last) to their average value
		// we will use the same number of votes in the test
		for i := range histVotes {
			if i < len(histVotes)-1 {
				histNVotes += (histVotes[i].Bin + histVotes[i+1].Bin) / 2 * histVotes[i].Count
			} else {
				histNVotes += histVotes[i].Bin * histVotes[i].Count
			}
		}

		// compute the proportion of elements in each bin
		// we will compare these to the proportions resulting from the test
		histElemProportions := make(map[int64]float32, 0)
		for _, bin := range histVotes {
			histElemProportions[bin.Bin] = float32(bin.Count) / float32(histNElements)
		}

		// create a sample, and sample from it,
		// keeping track of the number of votes of each element
		votes := make(map[int64]int64, 0)
		sampler := NewSampler(histVotes)
		for i := int64(0); i < histNVotes; i++ {
			votes[sampler.Sample()]++
		}

		// create a new histogram by copying the bins from the input histogram
		// (but leaving counts to 0)
		sampleVotes := make([]struct {
			Bin   int64
			Count int64
		}, len(histVotes))
		for i := range histVotes {
			sampleVotes[i].Bin = histVotes[i].Bin
		}

		// compute the resulting histogram
		for _, vCount := range votes {
			if vCount > sampleVotes[len(sampleVotes)-1].Bin {
				sampleVotes[len(sampleVotes)-1].Count++
			} else {
				for i := range sampleVotes {
					if vCount < sampleVotes[i].Bin {
						sampleVotes[i-1].Count++
						break
					}
				}
			}
		}

		// compute the number of elements in the resulting histogram
		for _, bin := range sampleVotes {
			sampleNElements += bin.Count
		}
		// a 5% difference should be ok
		assert.Greater(t, float64(histNElements)/20, math.Abs(float64(histNElements-sampleNElements)), "")

		// compute the proportion of elements in each bin in the resulting histogram
		sampleElemProportions := make(map[int64]float32, 0)
		for _, bin := range sampleVotes {
			sampleElemProportions[bin.Bin] = float32(bin.Count) / float32(sampleNElements)
		}

		// Extract and sort the bins from the input (and resulting) histogram.
		// We sort because we want to differentiate the 1st and 2nd bins.
		bins := make([]int64, len(histElemProportions))
		i := 0
		for bin := range histElemProportions {
			bins[i] = bin
			i++
		}
		sort.Slice(bins, func(i, j int) bool { return bins[i] < bins[j] })

		// examine how close the resulting historam's proportions are to the
		// input histgram ones.
		// For bins other than the 1st and 2nd, 1% diff is ok.
		// There are some leaks from the 1st to the 2nd, so 5% for those two.
		for _, bin := range bins {
			if bin == bins[0] || bin == bins[1] {
				assert.Greater(t, 0.05, math.Abs(float64(histElemProportions[bin]-sampleElemProportions[bin])))
			} else {
				assert.Greater(t, 0.01, math.Abs(float64(histElemProportions[bin]-sampleElemProportions[bin])))
			}
		}
	}
}
