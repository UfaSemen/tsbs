package peregreen

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/siemens"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

// BaseGenerator contains settings specific for Peregreen
type BaseGenerator struct {
}

// GenerateEmptyQuery returns an empty query.HTTP.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, method, body, path string) {
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte(method)
	q.Path = []byte(path)
	q.Body = []byte(body)
}

// NewSiemens creates a new siemens use case query generator.
func (g *BaseGenerator) NewSiemens(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := siemens.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	siemens := &Siemens{
		BaseGenerator: g,
		Core:          core,
	}

	return siemens, nil
}
