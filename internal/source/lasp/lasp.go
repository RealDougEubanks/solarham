// Package lasp polls the Laboratory for Atmospheric and Space Physics at the
// University of Colorado Boulder, which serves two unrelated products behind
// one hostname.
//
// # SDO/EVE Level 0CS — the reason this source exists
//
// EVE's quicklook file carries EUV irradiance in five bands at one-minute
// cadence with about a minute of latency, which is already better than anything
// NOAA publishes. But the standout columns are CMLat and CMLon: the
// heliographic latitude and longitude of the centroid of EUV emission, once a
// minute. That is, in effect, where on the disc the dominant activity is right
// now — a longitude near zero means a flare's effects reach Earth most directly
// — and it is published nowhere else, at any cadence, by anybody.
//
// # LISIRD
//
// The LISIRD collection is the slow half: total solar irradiance, the Bremen
// Mg II core-to-wing index, composite Lyman-alpha, and the CLS multi-frequency
// radio flux series at 8, 15, 30, 32 and 10.7 cm. All daily or six-hourly, all
// tiny — 600 bytes to 5 KB per response.
//
// # Dead URLs
//
// The addresses commonly cited for the EVE quicklook file do not work. The
// working path uses "evewebdata" rather than "eve_data" or "data", and
// "DIODES" is plural:
//
//	/eve/data_access/evewebdata/quicklook/L0CS/LATEST_EVE_L0CS_DIODES_1m.txt
//
// # Sentinels
//
// EVE marks missing data as -1.00e+00, stated in its own header. This is not a
// rare condition: the 121.6 nm MEGS-P channel reports the sentinel on nearly
// every row because that instrument has degraded, and the 36.6 nm ESP channel
// does too. An exporter that published those would show a permanent irradiance
// of minus one watt per square metre, so any column at the sentinel produces no
// sample at all.
//
// LISIRD has no declared sentinel, but tsis_tsi_6hr writes an all-zero row for
// a six-hour bucket it has no data for. A total solar irradiance of zero is not
// a measurement of a dark Sun; it is a placeholder, and it is filtered the same
// way.
//
// # Licence and citation
//
// LASP's data is NASA-funded and effectively open, with no registration and no
// stated restriction. It asks to be cited per instrument team:
//
//   - SDO/EVE: Woods, T. N. et al., 2012. Extreme Ultraviolet Variability
//     Experiment (EVE) on the Solar Dynamics Observatory (SDO). Solar Physics
//     275, 115. Data from the SDO/EVE Science Processing and Operations Center,
//     LASP/CU.
//   - TSIS-1 TIM total solar irradiance: the TSIS-1 team, LASP/CU.
//   - Bremen composite Mg II index: Snow, M., Weber, M. et al., University of
//     Bremen and LASP.
//   - Composite Lyman-alpha: Machol, J., Snow, M. et al., LASP/CU and NOAA
//     NCEI.
//   - CLS multi-frequency radio flux: Collecte Localisation Satellites (CLS),
//     Toulouse, redistributed through LISIRD.
//   - penticton_radio_flux: originates with the Dominion Radio Astrophysical
//     Observatory and Natural Resources Canada. See the note on it in
//     lisird.go: it is a republication of what the drao source fetches
//     first-hand, and the two must not both be enabled.
package lasp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It
// becomes a metric label, so it is stable.
const Name = "lasp"

const (
	// defaultBaseURL is the university host. Tests override it; nothing outside
	// this package can.
	defaultBaseURL = "https://lasp.colorado.edu"

	// defaultRetries is retries *after* the first attempt.
	defaultRetries = 2
)

// Source polls the LASP services.
type Source struct {
	baseURL string
	eve     bool
	lisird  bool
	retries int
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. Only the data-age computation and the
	// LISIRD query window need it; every published timestamp comes from the
	// payload.
	now func() time.Time

	// eveDate caches the UT date line from the EVE file's header, so the header
	// is fetched once a day rather than once a minute. See eve.go.
	eveDate eveDateCache

	// f107 adds the two LISIRD datasets that duplicate the drao source's
	// series. Off by default; see f107Datasets in lisird.go.
	f107 bool

	// lisirdMu guards lisirdLast, the last successful LISIRD fetch. Poll runs
	// on the scheduler's per-source goroutine, but the health endpoints may
	// read the source concurrently.
	lisirdMu   sync.Mutex
	lisirdLast time.Time
}

// Compile-time proof this satisfies what the scheduler polls.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds a source from validated configuration.
//
// A configuration with neither product enabled is rejected rather than
// defaulted, because there is no sensible default: a source that fetches
// nothing would report success forever while publishing no series, which is the
// hardest kind of misconfiguration to notice. Everything else is defaulted.
func New(cfg config.LASP, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("lasp: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if !cfg.EVE && !cfg.LISIRD {
		return nil, errors.New("lasp: neither EVE nor LISIRD is enabled, so this source would fetch nothing")
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("lasp: base URL is not absolute: %q", base)
	}

	// cfg.Timeout is deliberately not read. Per-request timeouts belong to the
	// shared httpx client, which owns them for every source at once; honouring a
	// per-source value here would need a second http.Client and would bypass the
	// shared rate limiter, which is the one thing this package must not do. The
	// setting is left in the config struct because an operator who sets it
	// expects it to mean something, and the exporter's single HTTP timeout is where it now lives.

	// Zero is "unset", not "never retry". A negative value is the only way to
	// express "one attempt" on an int field, so it is honoured.
	retries := cfg.Retries
	switch {
	case retries < 0:
		retries = 0
	case retries == 0:
		retries = defaultRetries
	}

	return &Source{
		baseURL: base,
		eve:     cfg.EVE,
		lisird:  cfg.LISIRD,
		retries: retries,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls, used for the staleness window
// and the failure backoff.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls at the cadence of the fastest enabled product.
//
// The two products want wildly different schedules — one minute against three
// hours — and they are combined into one source rather than split into two
// because that is what the configuration describes. Poll therefore does the
// rate-limiting itself: it fetches EVE on every tick and LISIRD only when its
// own interval has elapsed, so enabling LISIRD alongside EVE does not turn a
// daily dataset into 1,440 requests a day.
//
// When only LISIRD is enabled the schedule drops to its own interval, so the
// per-poll gating never fires and the source is polled eight times a day
// instead of 1,440.
func (s *Source) Schedule() source.Schedule {
	if s.eve {
		return source.Every(eveInterval)
	}
	// AtLeast makes the hourly floor part of the schedule rather than a rule
	// that only validation enforces. A courtesy limit an operator can override
	// by editing a setting is not a limit.
	return source.AtLeast(lisirdMinInterval, source.Every(lisirdInterval))
}

// Poll fetches whichever products are enabled and due.
//
// A failure in one product does not fail the poll if the other produced
// samples: an operator would far rather still have the minute-cadence EUV
// centroid than lose it because a daily Mg II dataset is briefly 500ing. The
// poll fails only when everything it attempted failed.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: fetched}

	var (
		errs        []error
		attempted   int
		notModified int
		newest      time.Time
	)

	note := func(observed time.Time) {
		if observed.After(newest) {
			newest = observed
		}
	}

	if s.eve {
		attempted++
		samples, observed, err := s.pollEVE(ctx)
		switch {
		case errors.Is(err, httpx.ErrNotModified):
			notModified++
		case errors.Is(err, httpx.ErrRateLimited):
			return metric.Batch{Source: Name, Fetched: fetched},
				fmt.Errorf("%w: %v", source.ErrRateLimited, err)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return metric.Batch{Source: Name, Fetched: fetched}, fmt.Errorf("lasp: eve: %w", err)
		case err != nil:
			errs = append(errs, fmt.Errorf("eve: %w", err))
		default:
			batch.Samples = append(batch.Samples, samples...)
			note(observed)
		}
	}

	if s.lisird && s.lisirdDue(fetched) {
		attempted++
		samples, observed, err := s.pollLISIRD(ctx, fetched)
		switch {
		case errors.Is(err, httpx.ErrRateLimited):
			return metric.Batch{Source: Name, Fetched: fetched},
				fmt.Errorf("%w: %v", source.ErrRateLimited, err)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return metric.Batch{Source: Name, Fetched: fetched}, fmt.Errorf("lasp: lisird: %w", err)
		case err != nil:
			errs = append(errs, fmt.Errorf("lisird: %w", err))
		default:
			batch.Samples = append(batch.Samples, samples...)
			note(observed)
			s.noteLISIRDPolled(fetched)
		}
	}

	if attempted > 0 && len(errs) == attempted {
		return metric.Batch{Source: Name, Fetched: fetched},
			fmt.Errorf("lasp: everything attempted failed: %w", errors.Join(errs...))
	}
	for _, err := range errs {
		s.log.Warn("lasp product failed, continuing with the rest", "error", err)
	}
	if attempted > 0 && notModified == attempted {
		return metric.Batch{Source: Name, Fetched: fetched}, source.ErrNotModified
	}

	// Age comes from the newest payload timestamp across the products, never
	// from Last-Modified. The EVE file is rewritten every minute regardless of
	// whether SDO downlinked anything, so its modification time is a statement
	// about the publisher's cron and not about the data.
	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			age = 0
		}
		s.add(&batch, metric.SourceDataAge, age, fetched, Name)
	}

	return batch, nil
}

// add appends a validated sample, dropping and logging an invalid one.
//
// A sample whose label values do not match its descriptor is rejected by
// Prometheus at collection time and silently corrupts InfluxDB tag sets, so it
// is caught at the source rather than in a sink.
func (s *Source) add(b *metric.Batch, desc *metric.Descriptor, value float64, at time.Time, labels ...string) {
	sample := metric.Sample{Desc: desc, Labels: labels, Value: value, Time: at}
	if err := sample.Validate(); err != nil {
		s.log.Warn("lasp produced an invalid sample; dropping it", "error", err)
		return
	}
	b.Samples = append(b.Samples, sample)
}
