package reduce

import (
	"context"

	alog "github.com/apex/log"

	"gitlab.com/bnoon/datechan"
	"gitlab.com/bnoon/griddata"
)

type dataListItem struct {
	data griddata.DataChunk
	next *dataListItem
}

func None(ctx context.Context,
	config Config,
	drc datechan.DateRangeChannel,
	inData, outData chan griddata.DataChunk) error {

	defer close(outData)

	var (
		dr             datechan.DateIdxRange
		last_start     datechan.DateIdx
		inDC, inDC1    griddata.DataChunk
		dr_ok, inDC_ok bool
		obsCnt         int
	)

	nextRange := func() error {
		if obsCnt > 0 {
			last_start = dr.Start.Copy()
			outDC := griddata.DataChunk{
				Date:   dr.Resample(inDC1.Date),
				Offset: inDC1.Offset,
				Length: inDC1.Length,
				Data:   nil}
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
			// alog.Debugf("add %s", inDC.Date.Key())
			obsCnt++
			inDC1 = inDC
		}
		if !inDC.Date.Less(dr.End) {
			// alog.Debugf("done? %d %s >= %s", obsCnt, inDC.Date.Key(), dr.End.Key())
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
