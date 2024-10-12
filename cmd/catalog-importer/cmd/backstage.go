package cmd

import (
	"context"
	"encoding/json"

	"github.com/alecthomas/kingpin/v2"
	kitlog "github.com/go-kit/kit/log"
	"github.com/incident-io/catalog-importer/v2/config"
	"github.com/incident-io/catalog-importer/v2/docs"
	"github.com/incident-io/catalog-importer/v2/source"
	"github.com/pkg/errors"
)

type BackstageOptions struct {
	APIEndpoint       string
	APIKey            string
	BackstageEndpoint string
	BackstageToken    string
	BackstageSignJWT  bool
	Filters           string
	Limit             int
	Offset            int
}

func (opt *BackstageOptions) Bind(cmd *kingpin.CmdClause) *BackstageOptions {
	cmd.Flag("api-endpoint", "Endpoint of the incident.io API").
		Default("https://api.incident.io").
		Envar("INCIDENT_ENDPOINT").
		StringVar(&opt.APIEndpoint)
	cmd.Flag("api-key", "API key for incident.io").
		Envar("INCIDENT_API_KEY").
		StringVar(&opt.APIKey)
	cmd.Flag("backstage-endpoint", "Endpoint of the Backstage entries API").
		Default("http://localhost:7007/api/catalog/entities/by-query").
		Envar("BACKSTAGE_ENDPOINT").
		StringVar(&opt.BackstageEndpoint)
	cmd.Flag("backstage-token", "Token of the Backstage entries API. Optional").
		Default("").
		Envar("BACKSTAGE_TOKEN").
		StringVar(&opt.BackstageToken)
	cmd.Flag("backstage-sign-jwt", "Whether to sign the provided Backstage token into a JWT. Defaults to true").
		Default("true").
		Envar("BACKSTAGE_SIGN_JWT").
		BoolVar(&opt.BackstageSignJWT)
	cmd.Flag("filters", "Query filters for the Backstage entities API").
		Default("").
		Envar("BACKSTAGE_FILTERS").
		StringVar(&opt.Filters)
	cmd.Flag("limit", "Limit for the Backstage entities API").
		Default("0").
		Envar("BACKSTAGE_LIMIT").
		IntVar(&opt.Limit)
	cmd.Flag("offset", "Offset for the Backstage entities API").
		Default("0").
		Envar("BACKSTAGE_OFFSET").
		IntVar(&opt.Offset)

	return opt
}

func (opt *BackstageOptions) Run(ctx context.Context, logger kitlog.Logger) error {
	data, err := docs.EvaluateJsonnet("backstage", "importer.jsonnet")
	if err != nil {
		return err
	}

	var cfg config.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return errors.Wrap(err, "parsing Backstage config")
	}

	// Find the local file pipeline and change the source to point at Backstage.
	for _, pipeline := range cfg.Pipelines {
		if len(pipeline.Sources) > 0 && pipeline.Sources[0].Local != nil {
			pipeline.Sources = []*source.Source{
				{
					Backstage: &source.SourceBackstage{
						Endpoint: opt.BackstageEndpoint,
						Token:    source.Credential(opt.BackstageToken),
						SignJWT:  &opt.BackstageSignJWT,
						Filters:  opt.Filters,
						Limit:    opt.Limit,
						Offset:   opt.Offset,
					},
				},
			}
		}
	}

	syncOpt := *syncOptions
	syncOpt.APIEndpoint = opt.APIEndpoint
	syncOpt.APIKey = opt.APIKey
	syncOpt.AllowDeleteAll = true

	if err := syncOpt.Run(ctx, logger, &cfg); err != nil {
		return errors.Wrap(err, "running sync")
	}

	return nil
}
