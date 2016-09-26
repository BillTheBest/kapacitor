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

type ConfigUpdate struct {
	Name      string
	NewConfig interface{}
}

type Service struct {
	overrider *override.Overrider
	logger    *log.Logger
	updates   chan<- ConfigUpdate

	overrides OverrideDAO

	StorageService interface {
		Store(namespace string) storage.Interface
	}
}

func NewService(config interface{}, l *log.Logger, updates chan<- ConfigUpdate) *Service {
	cu := override.New(config)
	cu.FieldNameFunc = override.TomlFieldName
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
	return nil
}

func (s *Service) Close() error {
	close(s.updates)
	return nil
}

type updateAction struct {
	Set    map[string]interface{} `json:"set"`
	Delete []string               `json:"delete"`
}

func (s *Service) handleUpdateRequest(w http.ResponseWriter, r *http.Request) {
	section, name := path.Split(r.URL.Path)
	if section == "/" {
		section = name
		name = ""
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
	newConfig, err := s.overrider.Override(section, name, set)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update config:", err), true, http.StatusBadRequest)
		return
	}
	cu := ConfigUpdate{
		Name:      section,
		NewConfig: newConfig,
	}
	s.updates <- cu
	w.WriteHeader(http.StatusNoContent)
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

	if err := s.overrides.Set(o); err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
	}
	return o.Overrides, nil
}

// getConfig returns a map of a fully resolved configuration object.
func (s *Service) getConfig() (map[string]interface{}, error) {
	overrides, err := s.overrides.List()
	if err != nil {
		return errors.Wrap(err, "failed to retrieve config overrides")
	}
	config := make(map[string]interface{}, len(overrides))

	for _, o := range overrides {
	}
	return nil, nil
}
