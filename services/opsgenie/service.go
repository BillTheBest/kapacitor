package opsgenie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	mu           sync.RWMutex
	enabled      bool
	apikey       string
	teams        []string
	recipients   []string
	url          string
	recovery_url string
	global       bool
	logger       *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		enabled:      c.Enabled,
		teams:        c.Teams,
		recipients:   c.Recipients,
		apikey:       c.APIKey,
		url:          c.URL + "/",
		recovery_url: c.RecoveryURL + "/",
		global:       c.Global,
		logger:       l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.mu.Lock()
		s.teams = c.Teams
		s.recipients = c.Recipients
		s.apikey = c.APIKey
		s.url = c.URL + "/"
		s.recovery_url = c.RecoveryURL + "/"
		s.global = c.Global
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) Global() bool {
	return s.global
}

func (s *Service) Alert(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) error {
	url, post, err := s.preparePost(teams, recipients, messageType, message, entityID, t, details)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Message string `json:"message"`
		}
		r := &response{Message: fmt.Sprintf("failed to understand OpsGenie response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Message)
	}
	return nil
}

func (s *Service) preparePost(teams []string, recipients []string, messageType, message, entityID string, t time.Time, details interface{}) (string, io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.enabled {
		return "", nil, errors.New("service not enabled")
	}

	ogData := make(map[string]interface{})
	url := s.url

	ogData["apiKey"] = s.apikey
	ogData["entity"] = entityID
	ogData["alias"] = entityID
	ogData["message"] = message
	ogData["note"] = ""
	ogData["monitoring_tool"] = kapacitor.Product

	//Extra Fields (can be used for filtering)
	ogDetails := make(map[string]interface{})
	ogDetails["Level"] = messageType
	ogDetails["Monitoring Tool"] = "Kapacitor"

	ogData["details"] = ogDetails

	switch messageType {
	case "RECOVERY":
		url = s.recovery_url
		ogData["note"] = message
	}

	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return "", nil, err
		}
		ogData["description"] = string(b)
	}

	if len(teams) == 0 {
		teams = s.teams
	}

	if len(teams) > 0 {
		ogData["teams"] = teams
	}

	if len(recipients) == 0 {
		recipients = s.recipients
	}

	if len(recipients) > 0 {
		ogData["recipients"] = recipients
	}

	// Post data to VO
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(ogData)
	if err != nil {
		return "", nil, err
	}

	return url, &post, nil
}
