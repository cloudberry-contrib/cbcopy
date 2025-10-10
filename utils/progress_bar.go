package utils

/*
 * This file contains structs and functions related to logging.
 */

import (
	"time"

	"github.com/apache/cloudberry-go-libs/gplog"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

/*
 * Progress bar functions
 */

/*
 * The following constants are used for determining when to display a progress bar
 *
 * PB_INFO only shows in info mode because some methods have a different way of
 * logging in verbose mode and we don't want them to conflict
 * PB_VERBOSE show a progress bar in INFO and VERBOSE mode
 *
 * A simple incremental progress tracker will be shown in info mode and
 * in verbose mode we will log progress at increments of 10%
 */
const (
	PB_NONE = iota
	PB_INFO
	PB_VERBOSE

	//Verbose progress bar logs every 5 percent
	INCR_PERCENT = 5
)

func NewProgressBar(count int, prefix string, showProgressBar int) ProgressBar {
	progress := mpb.New(mpb.WithWidth(60), mpb.WithRefreshRate(180*time.Millisecond))
	return newProgressBar(progress, count, prefix, showProgressBar)
}

func NewProgressBarEx(progress *mpb.Progress, count int, prefix string) ProgressBar {
	return newProgressBar(progress, count, prefix, PB_VERBOSE)
}

func newProgressBar(progress *mpb.Progress, count int, prefix string, showProgressBar int) ProgressBar {
	format := prefix + " (%d/%d)"

	progressBar := progress.AddBar(int64(count), mpb.BarStyle(mpb.DefaultBarStyle),
		mpb.PrependDecorators(decor.Counters(0, format)),
		mpb.AppendDecorators(decor.Percentage()),
	)

	if showProgressBar == PB_VERBOSE {
		return NewVerboseProgressBar(count, prefix, progressBar, progress)
	}

	epb := &ExtendProgressBar{Bar: progressBar, Progress: progress}
	return epb
}

type ProgressBar interface {
	Start()
	Finish()
	Increment()
}

type VerboseProgressBar struct {
	current            int
	total              int
	prefix             string
	nextPercentToPrint int
	*mpb.Bar
	*mpb.Progress
}

type ExtendProgressBar struct {
	*mpb.Bar
	*mpb.Progress
}

func (epb *ExtendProgressBar) Increment() { epb.Bar.Increment() }
func (epb *ExtendProgressBar) Start()     {}
func (epb *ExtendProgressBar) Finish()    { epb.Progress.Wait() }

func NewVerboseProgressBar(count int, prefix string, bar *mpb.Bar, progress *mpb.Progress) *VerboseProgressBar {
	newPb := VerboseProgressBar{total: count, prefix: prefix, nextPercentToPrint: INCR_PERCENT, Bar: bar, Progress: progress}
	return &newPb
}

func (vpb *VerboseProgressBar) Increment() {
	vpb.Bar.Increment()
	if vpb.current < vpb.total {
		vpb.current++
		vpb.checkPercent()
	}
}

func (vpb *VerboseProgressBar) Start()  {}
func (vpb *VerboseProgressBar) Finish() { vpb.Progress.Wait() }

/*
 * If progress bar reaches a percentage that is a multiple of 10, log a message to stdout
 * We increment nextPercentToPrint so the same percentage will not be printed multiple times
 */
func (vpb *VerboseProgressBar) checkPercent() {
	currPercent := int(float64(vpb.current) / float64(vpb.total) * 100)
	//closestMult is the nearest percentage <= currPercent that is a multiple of 10
	closestMult := currPercent / INCR_PERCENT * INCR_PERCENT
	if closestMult >= vpb.nextPercentToPrint {
		vpb.nextPercentToPrint = closestMult
		gplog.Verbose("%s %d%% (%d/%d)", vpb.prefix, vpb.nextPercentToPrint, vpb.current, vpb.total)
		vpb.nextPercentToPrint += INCR_PERCENT
	}
}
