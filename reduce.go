package reduce

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"gitlab.com/bnoon/datechan"
	"gitlab.com/bnoon/griddata"
	"gitlab.com/bnoon/griddata/params"
)

type Config struct {
	Name string
	Func func(
		context.Context,
		Config,
		datechan.DateRangeChannel,
		chan griddata.DataChunk,
		chan griddata.DataChunk) error
	Overlapping    bool
	DateRanges     chan datechan.DateRangeChannel
	MaxMissing     int
	Threshold      string
	ThresholdValue float32
}

var (
	threshold_pattern *regexp.Regexp = regexp.MustCompile(`^(cnt)_(lt|gt|le|ge|eq)_([-+]?\d*\.?\d*)$`)

// threshold_pattern *regexp.Regexp = regexp.MustCompile(`^(cnt|pct|fct)_(eq|lt|le|gt|ge|ne)_([-+]?\d*\.?\d*)$`)
)

func Setup(elem params.Element) (Config, error) {
	cfg := Config{
		Name:        elem.ReduceDef,
		Overlapping: elem.DateIterConfig.IsOverlapping(),
		MaxMissing:  elem.MaxMissing,
	}
	if cfg.Name == "mean" {
		if cfg.Overlapping {
			cfg.Func = MeanOverlap
		} else {
			cfg.Func = Mean
		}
		return cfg, nil
	}

	if cfg.Name == "sum" {
		if cfg.Overlapping {
			cfg.Func = SumOverlap
		} else {
			cfg.Func = Sum
		}
		return cfg, nil
	}

	tHold := threshold_pattern.FindStringSubmatch(cfg.Name)
	if len(tHold) > 0 {
		tVal, err := strconv.ParseFloat(tHold[3], 32)
		if err != nil {
			return cfg, fmt.Errorf("invalid threshold")
		}
		cfg.Threshold = tHold[2]
		cfg.ThresholdValue = float32(tVal)
		if cfg.Overlapping {
			cfg.Func = ThresholdOverlap
		} else {
			cfg.Func = Threshold
		}
		return cfg, nil
	}

	return cfg, fmt.Errorf("unknown reduction")
}
