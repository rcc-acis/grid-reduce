package reduce

import (
	"context"
	"math"

	alog "github.com/apex/log"

	"gitlab.com/bnoon/datechan"
	"gitlab.com/bnoon/griddata"
)

func Sum(ctx context.Context,
	config Config,
	drc datechan.DateRangeChannel,
	inData, outData chan griddata.DataChunk) error {

	defer close(outData)

	nan := float32(math.NaN())

	var (
		dr             datechan.DateIdxRange
		last_start     datechan.DateIdx
		inDC, inDC1    griddata.DataChunk
		dr_ok, inDC_ok bool
		obsCnt         int
		sum            []float32
		cnt            []int
	)

	nextRange := func() error {
		if obsCnt > 0 {
			expCnt := dr.Len()
			res := make([]float32, len(sum))
			for idx, v := range sum {
				if expCnt-cnt[idx] > config.MaxMissing {
					res[idx] = nan
				} else {
					res[idx] = v
				}
			}

			last_start = dr.Start.Copy()
			outDC := griddata.DataChunk{
				Date:   dr.Resample(inDC1.Date),
				Offset: inDC1.Offset,
				Length: inDC1.Length,
				Data:   res}

			select {
			case outData <- outDC:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case dr, dr_ok = <-drc:
		}

		if !(dr_ok && dr.Start.Equal(last_start)) {
			sum = nil
			obsCnt = 0
		}
		return nil
	}

	if err := nextRange(); err != nil {
		return err
	}

dataLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inDC, inDC_ok = <-inData:
		}
		if !inDC_ok {
			break
		}

		if inDC.Date.Less(dr.Start) {
			alog.Debugf("skip %s < %s", inDC.Date.Key(), dr.Start.Key())
			continue
		}
		if inDC.Date.Less(dr.End) || inDC.Date.Equal(dr.End) {
			// alog.Debugf("add %s",inDC.Date.Key())
			if sum == nil {
				sum = make([]float32, len(inDC.Data))
				cnt = make([]int, len(inDC.Data))
			}
			for idx, v := range inDC.Data {
				if v == v {
					sum[idx] += v
					cnt[idx]++
				}
			}
			obsCnt++
			inDC1 = inDC
		}
		if !inDC.Date.Less(dr.End) {
			// alog.Debugf("done? %d %s >= %s",obsCnt, inDC.Date.Key(), dr.End.Key())
			for {
				if err := nextRange(); err != nil {
					return err
				}
				if !dr_ok {
					break dataLoop
				}
				if !dr.End.Less(inDC.Date) {
					break
				}
				alog.Debugf("skip+ %s >= %s", inDC.Date.Key(), dr.End.Key())
			}
		}
	}

	if err := nextRange(); err != nil {
		return err
	}

	// drain data channel??
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inDC, inDC_ok = <-inData:
			if !inDC_ok {
				return nil
			}
		}
	}

	return nil
}

func SumOverlap(ctx context.Context,
	config Config,
	drc datechan.DateRangeChannel,
	inData, outData chan griddata.DataChunk) error {

	defer close(outData)

	nan := float32(math.NaN())

	var (
		dr              datechan.DateIdxRange
		inDC            griddata.DataChunk
		dr_ok, inDC_ok  bool
		obsCnt          int
		sum             []float32
		cnt             []int
		firstDC, lastDC *dataListItem
	)

	nextRange := func() error {
		if obsCnt > 0 {
			expCnt := dr.Len()
			res := make([]float32, len(sum))
			for idx, v := range sum {
				if expCnt-cnt[idx] > config.MaxMissing {
					res[idx] = nan
				} else {
					res[idx] = v
				}
			}
			outDC := griddata.DataChunk{
				Date:   dr.Resample(lastDC.data.Date),
				Offset: lastDC.data.Offset,
				Length: lastDC.data.Length,
				Data:   res}

			select {
			case outData <- outDC:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case dr, dr_ok = <-drc:
		}

		if dr_ok {
			for firstDC != nil {
				if firstDC.data.Date.Less(dr.Start) { // no longer in daterange
					// handle removing data:
					//   subtract data for sum/mean reduction
					//   test threshold and adjust count
					//   min/max, just remove
					for idx, v := range firstDC.data.Data {
						if v == v {
							sum[idx] -= v
							cnt[idx]--
						}
					}
					obsCnt--
					firstDC = firstDC.next
					if firstDC == nil {
						lastDC = nil
						sum = nil
					}
				} else {
					break
				}
			}
		} else {
			// obsCnt is a flag
			obsCnt = 0
		}
		return nil
	}

	if err := nextRange(); err != nil {
		return err
	}

dataLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inDC, inDC_ok = <-inData:
		}
		if !inDC_ok {
			break
		}

		if inDC.Date.Less(dr.Start) {
			alog.Debugf("skip %s < %s", inDC.Date.Key(), dr.Start.Key())
			continue
		}
		if inDC.Date.Less(dr.End) || inDC.Date.Equal(dr.End) {
			if sum == nil {
				sum = make([]float32, len(inDC.Data))
				cnt = make([]int, len(inDC.Data))
			}
			for idx, v := range inDC.Data {
				if v == v {
					sum[idx] += v
					cnt[idx]++
				}
			}
			obsCnt++
			if firstDC == nil {
				firstDC = &dataListItem{data: inDC}
				lastDC = firstDC
			} else {
				nextDC := &dataListItem{data: inDC}
				lastDC.next = nextDC
				lastDC = nextDC
			}
		}
		if !inDC.Date.Less(dr.End) {
			for {
				if err := nextRange(); err != nil {
					return err
				}
				if !dr_ok {
					break dataLoop
				}
				if !dr.End.Less(inDC.Date) {
					break
				}
				alog.Debugf("skip+ %s >= %s", inDC.Date.Key(), dr.End.Key())
			}
		}
	}

	if err := nextRange(); err != nil {
		return err
	}

	// drain data channel??
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inDC, inDC_ok = <-inData:
			if !inDC_ok {
				return nil
			}
		}
	}

	return nil
}
