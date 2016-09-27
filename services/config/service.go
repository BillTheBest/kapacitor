package config

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"

	"github.com/influxdata/kapacitor/services/config/override"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

const (
	configPath         = "/config"
	configPathAnchored = "/config/"
)

type ConfigUpdate struct {
	Name      string
	NewConfig interface{}
}

type Service struct {
	overrider *override.Overrider
	logger    *log.Logger
	updates   chan<- ConfigUpdate
	routes    []httpd.Route

	overrides OverrideDAO

	StorageService interface {
		Store(namespace string) storage.Interface
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(config interface{}, l *log.Logger, updates chan<- ConfigUpdate) *Service {
	cu := override.New(config)
	cu.OptionNameFunc = override.TomlFieldName
	return &Service{
		overrider: cu,
		logger:    l,
		updates:   updates,
	}
}

// The storage namespace for all configuration override data.
const configNamespace = "config_overrides"

func (s *Service) Open() error {
	store := s.StorageService.Store(configNamespace)
	s.overrides = newOverrideKV(store)

	// Define API routes
	s.routes = []httpd.Route{
		{
			Name:        "config",
			Method:      "GET",
			Pattern:     configPath,
			HandlerFunc: s.handleGetConfig,
		},
		{
			Name:        "config",
			Method:      "POST",
			Pattern:     configPathAnchored,
			HandlerFunc: s.handleUpdateSection,
		},
	}

	err := s.HTTPDService.AddRoutes(s.routes)
	return errors.Wrap(err, "failed to add API routes")
}

func (s *Service) Close() error {
	close(s.updates)
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

type updateAction struct {
	Set    map[string]interface{} `json:"set"`
	Delete []string               `json:"delete"`
}

func (s *Service) handleUpdateSection(w http.ResponseWriter, r *http.Request) {
	section := path.Base(r.URL.Path)
	if section == "" {
		httpd.HttpError(w, "must provide section name", true, http.StatusBadRequest)
		return
	}
	var ua updateAction
	err := json.NewDecoder(r.Body).Decode(&ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to decode JSON:", err), true, http.StatusBadRequest)
		return
	}

	// Apply sets/deletes to stored overrides
	set, err := s.applyUpdateAction(section, ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update config:", err), true, http.StatusBadRequest)
		return
	}

	// Apply overrides to config object
	newConfig, err := s.overrider.Override(section, set)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update config:", err), true, http.StatusBadRequest)
		return
	}
	cu := ConfigUpdate{
		Name:      section,
		NewConfig: newConfig.Value(),
	}
	s.updates <- cu
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	config, err := s.getConfig()
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to resolve current config:", err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(config)
}

func (s *Service) applyUpdateAction(id string, ua updateAction) (map[string]interface{}, error) {
	o, err := s.overrides.Get(id)
	if err == ErrNoOverrideExists {
		o = Override{
			ID:        id,
			Overrides: make(map[string]interface{}),
		}
	} else if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
	}
	for k, v := range ua.Set {
		o.Overrides[k] = v
	}
	for _, k := range ua.Delete {
		delete(o.Overrides, k)
	}

	s.logger.Println("D! setting override", o)
	if err := s.overrides.Set(o); err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
	}
	return o.Overrides, nil
}

// getConfig returns a map of a fully resolved configuration object.
func (s *Service) getConfig() (map[string]map[string]interface{}, error) {
	overrideList, err := s.overrides.List()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve config overrides")
	}
	overrides := make(map[string]Override, len(overrideList))
	for _, o := range overrideList {
		overrides[o.ID] = o
	}
	sections, err := s.overrider.Sections()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve config sections")
	}

	config := make(map[string]map[string]interface{}, len(sections))
	for name, section := range sections {
		if o, ok := overrides[name]; ok {
			newSection, err := s.overrider.Override(name, o.Overrides)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to override section %s", name)
			} else {
				section = newSection
			}
		}
		redacted, err := section.Redacted()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to redact section %s", name)
		}
		config[name] = redacted
	}
	return config, nil
}
