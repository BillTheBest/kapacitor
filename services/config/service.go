package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"

	"github.com/influxdata/kapacitor/services/config/configupdate"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
)

type ConfigUpdate struct {
	Name      string
	NewConfig interface{}
}

type Service struct {
	configUpdater *configupdate.ConfigUpdater
	logger        *log.Logger
	updates       chan<- ConfigUpdate

	//configOverrides ConfigOverrideDAO

	StorageService interface {
		Store(namespace string) storage.Interface
	}
}

func NewService(config interface{}, l *log.Logger, updates chan<- ConfigUpdate) *Service {
	cu := configupdate.New(config)
	cu.FieldNameFunc = configupdate.TomlFieldName
	return &Service{
		configUpdater: cu,
		logger:        l,
		updates:       updates,
	}
}

// The storage namespace for all configuration override data.
const configNamespace = "config_overrides"

func (s *Service) Open() error {
	//store := s.StorageService.Store(configNamespace)
	//s.configOverrides = newConfigOverrideKV(store)
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
	// TODO appy deletes to existing override map.
	newConfig, err := s.configUpdater.Update(section, name, ua.Set)
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

func (s *Service) getUpdatedConfig(section, name string, data io.Reader) (interface{}, error) {
	return nil, nil
}
