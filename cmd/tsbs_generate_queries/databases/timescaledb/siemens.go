package timescaledb

import "github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/siemens"

// Siemens produces TimescaleDB-specific queries for all the siemens query types.
type Siemens struct {
	*BaseGenerator
	*siemens.Core
}
