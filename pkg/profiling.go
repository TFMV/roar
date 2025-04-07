package pkg

import (
	"net/http"
	_ "net/http/pprof" // Import for side-effects to register pprof handlers
	"runtime"
	"sync"

	"github.com/rs/zerolog/log"
)

var (
	pprofOnce sync.Once
)

// StartProfiling starts the HTTP server for pprof profiling
func StartProfiling(addr string) {
	pprofOnce.Do(func() {
		// Enable block profiling
		runtime.SetBlockProfileRate(1000) // 1 sample per 1000 nanoseconds of blocking time

		// Enable mutex profiling
		runtime.SetMutexProfileFraction(5) // 1/5 of mutex contention events are reported

		// Start pprof server
		go func() {
			log.Info().Str("addr", addr).Msg("Starting pprof server")
			if err := http.ListenAndServe(addr, nil); err != nil {
				log.Error().Err(err).Msg("pprof server error")
			}
		}()
	})
}
