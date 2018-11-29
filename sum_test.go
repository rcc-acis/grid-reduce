package reduce

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/bnoon/datechan"
	"gitlab.com/bnoon/griddata"
	"gitlab.com/bnoon/griddata/params"
)

func TestSumBasic(t *testing.T) {
	assert := assert.New(t)
	cal := &datechan.Gregorian{}
	ctx := context.Background()

	var elem params.Element
	jsonBlob := []byte(`{"vX":4, "interval":[0,0,4], "duration":4, "reduce":"sum","maxMissing":1}`)
	err := json.Unmarshal(jsonBlob, &elem)
	assert.Nil(err)

	cfg, err := Setup(elem)

	drCfg := datechan.IDconfig{
		Interval:      elem.DateIterConfig.Interval,
		Duration:      elem.DateIterConfig.Duration,
		Sdate:         []int{2000, 1, 4},
		Edate:         []int{2000, 1, 4},
		Calendar:      cal,
		InResolution:  3,
		OutResolution: 3,
	}
	drCfg.Validate()
	assert.False(drCfg.IsOverlapping())
	drc := datechan.New(ctx, drCfg)

	inData := make(chan griddata.DataChunk, 10)
	outData := make(chan griddata.DataChunk, 0)
	// run reduction

	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 1}),
		Data: []float32{0, 0, 0, 0},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 2}),
		Data: []float32{1, 1, 1, 1},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 3}),
		Data: []float32{2, 2, 2, 2},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 4}),
		Data: []float32{3, 3, 3, 3},
	}
	close(inData)
	go func() {
		err := cfg.Func(ctx, cfg, drc, inData, outData)
		assert.Nil(err)
	}()
	d, ok := <-outData
	assert.True(ok)
	assert.Equal(4, len(d.Data))
	assert.Equal([]float32{6, 6, 6, 6}, d.Data)
}

func TestSumMissing1(t *testing.T) {
	assert := assert.New(t)
	cal := &datechan.Gregorian{}
	ctx := context.Background()

	var elem params.Element
	jsonBlob := []byte(`{"vX":4, "interval":[0,0,4], "duration":4, "reduce":"sum","maxMissing":0}`)
	err := json.Unmarshal(jsonBlob, &elem)
	assert.Nil(err)
	nan := float32(math.NaN())
	cfg, err := Setup(elem)

	drCfg := datechan.IDconfig{
		Interval:      elem.DateIterConfig.Interval,
		Duration:      elem.DateIterConfig.Duration,
		Sdate:         []int{2000, 1, 4},
		Edate:         []int{2000, 1, 4},
		Calendar:      cal,
		InResolution:  3,
		OutResolution: 3,
	}
	drCfg.Validate()
	assert.False(drCfg.IsOverlapping())
	drc := datechan.New(ctx, drCfg)

	inData := make(chan griddata.DataChunk, 10)
	outData := make(chan griddata.DataChunk, 0)
	// run reduction

	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 1}),
		Data: []float32{0, 0, 0, 0},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 2}),
		Data: []float32{1, 1, 1, 1},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 3}),
		Data: []float32{2, 2, 2, 2},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 4}),
		Data: []float32{nan, nan, nan, nan},
	}
	close(inData)
	go func() {
		err := cfg.Func(ctx, cfg, drc, inData, outData)
		assert.Nil(err)
	}()
	d, ok := <-outData
	assert.True(ok)
	assert.Equal(4, len(d.Data))
	assert.False(d.Data[0] == d.Data[0])
}

func TestSumMissingAll(t *testing.T) {
	assert := assert.New(t)
	cal := &datechan.Gregorian{}
	ctx := context.Background()

	var elem params.Element
	jsonBlob := []byte(`{"vX":4, "interval":[0,0,4], "duration":4, "reduce":"sum","maxMissing":1}`)
	err := json.Unmarshal(jsonBlob, &elem)
	assert.Nil(err)
	nan := float32(math.NaN())
	cfg, err := Setup(elem)

	drCfg := datechan.IDconfig{
		Interval:      elem.DateIterConfig.Interval,
		Duration:      elem.DateIterConfig.Duration,
		Sdate:         []int{2000, 1, 4},
		Edate:         []int{2000, 1, 4},
		Calendar:      cal,
		InResolution:  3,
		OutResolution: 3,
	}
	drCfg.Validate()
	assert.False(drCfg.IsOverlapping())
	drc := datechan.New(ctx, drCfg)

	inData := make(chan griddata.DataChunk, 10)
	outData := make(chan griddata.DataChunk, 0)
	// run reduction

	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 1}),
		Data: []float32{nan, nan, nan, nan},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 2}),
		Data: []float32{nan, nan, nan, nan},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 3}),
		Data: []float32{nan, nan, nan, nan},
	}
	inData <- griddata.DataChunk{
		Date: cal.YMDtoYI([]int{2000, 1, 4}),
		Data: []float32{nan, nan, nan, nan},
	}
	close(inData)
	go func() {
		err := cfg.Func(ctx, cfg, drc, inData, outData)
		assert.Nil(err)
	}()
	d, ok := <-outData
	assert.True(ok)
	assert.Equal(4, len(d.Data))
	assert.False(d.Data[0] == d.Data[0])
}

func TestSumMissingMonth(t *testing.T) {
	assert := assert.New(t)
	cal := &datechan.Gregorian{}
	ctx := context.Background()

	var elem params.Element
	jsonBlob := []byte(`{"vX":4, "interval":[0,1], "duration":1, "reduce":"sum","maxMissing":1}`)
	err := json.Unmarshal(jsonBlob, &elem)
	assert.Nil(err)
	nan := float32(math.NaN())
	cfg, err := Setup(elem)

	drCfg := datechan.IDconfig{
		Interval:      []int{0, 1},
		Sdate:         []int{2000, 1},
		Edate:         []int{2000, 1},
		Calendar:      cal,
		InResolution:  3,
		OutResolution: 2,
	}
	drc := datechan.New(ctx, drCfg)

	inData := make(chan griddata.DataChunk, 40)
	outData := make(chan griddata.DataChunk, 0)
	for day := 1; day < 32; day++ {
		switch day {
		case 1:
			inData <- griddata.DataChunk{
				Date: cal.YMDtoYI([]int{2000, 1, day}),
				Data: []float32{nan, nan, 1, 1},
			}
		case 2:
			inData <- griddata.DataChunk{
				Date: cal.YMDtoYI([]int{2000, 1, day}),
				Data: []float32{nan, 1, 1, 1},
			}
		default:
			inData <- griddata.DataChunk{
				Date: cal.YMDtoYI([]int{2000, 1, day}),
				Data: []float32{1, 1, 1, 1},
			}
		}

	}
	close(inData)
	go func() {
		err := cfg.Func(ctx, cfg, drc, inData, outData)
		assert.Nil(err)
	}()
	d, ok := <-outData
	assert.True(ok)
	assert.Equal(4, len(d.Data))
	assert.False(d.Data[0] == d.Data[0])
	assert.Equal(float32(30.), d.Data[1])
	assert.Equal(float32(31.), d.Data[2])
}
