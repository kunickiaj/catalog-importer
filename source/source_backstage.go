package source

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	kitlog "github.com/go-kit/kit/log"
	"github.com/go-ozzo/ozzo-validation/is"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/golang-jwt/jwt"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/pkg/errors"
)

type SourceBackstage struct {
	Endpoint string     `json:"endpoint"` // https://backstage.company.io/api/catalog/entities/by-query
	Token    Credential `json:"token"`
	SignJWT  *bool      `json:"sign_jwt"`
	Filters  string     `json:"filters,omitempty"`
	Limit    int        `json:"limit,omitempty"`
	Offset   int        `json:"offset,omitempty"`
}

func (s SourceBackstage) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Endpoint,
			validation.Required.Error("must provide an endpoint for fetching Backstage entries"),
			is.URL,
		),
	)
}

func (s SourceBackstage) String() string {
	return fmt.Sprintf("backstage (endpoint=%s)", s.Endpoint)
}

func (s SourceBackstage) Load(ctx context.Context, logger kitlog.Logger) ([]*SourceEntry, error) {
	var token string
	if s.Token != "" {
		// If not provided or explicitly enabled, sign the token into a JWT and use that as
		// the Authorization header.
		if s.SignJWT == nil || *s.SignJWT {
			var err error
			token, err = s.getJWT()
			if err != nil {
				return nil, err
			}
			// Otherwise if someone has told us not to, don't sign the token and use it as-is.
		} else {
			token = string(s.Token)
		}
	}

	client := cleanhttp.DefaultClient()

	entries := []*SourceEntry{}
	for {
		reqURL, err := url.Parse(s.Endpoint)
		if err != nil {
			return nil, errors.Wrap(err, "parsing Backstage endpoint URL")
		}

		query := reqURL.Query()
		if s.Filters != "" {
			query.Set("filter", s.Filters)
		}
		if s.Limit > 0 {
			query.Set("limit", fmt.Sprintf("%d", s.Limit))
		}
		if s.Offset > 0 {
			query.Set("offset", fmt.Sprintf("%d", s.Offset))
		}
		reqURL.RawQuery = query.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return nil, errors.Wrap(err, "building Backstage URL")
		}
		if token != "" {
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
		}

		resp, err := client.Do(req)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("received error from Backstage: %s", resp.Status)
		}
		if err != nil {
			return nil, errors.Wrap(err, "fetching Backstage entries")
		}

		page := []json.RawMessage{}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			return nil, errors.Wrap(err, "parsing Backstage entries")
		}

		if len(page) == 0 {
			return entries, nil
		}

		for idx := range page {
			entries = append(entries, &SourceEntry{
				Origin:  s.String(),
				Content: page[idx],
			})
		}

		s.Offset += len(page)
	}
}

// getJWT applies the rules from the Backstage docs to generate a JWT that is valid for
// external Backstage authentication.
//
// https://backstage.io/docs/auth/service-to-service-auth/#usage-in-external-callers
func (s SourceBackstage) getJWT() (string, error) {
	token := jwt.New(jwt.SigningMethodHS256)
	token.Claims = jwt.MapClaims{
		"sub": "backstage-server",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	secret, err := base64.StdEncoding.DecodeString(string(s.Token))
	if err != nil {
		return "", errors.Wrap(err, "supplied backstage token must be a base64 string")
	}

	return token.SignedString(secret)
}
