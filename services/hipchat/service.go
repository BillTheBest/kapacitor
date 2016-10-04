package hipchat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	mu               sync.RWMutex
	enabled          bool
	room             string
	token            string
	url              string
	global           bool
	stateChangesOnly bool
	logger           *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		enabled:          c.Enabled,
		room:             c.Room,
		token:            c.Token,
		url:              c.URL,
		global:           c.Global,
		stateChangesOnly: c.StateChangesOnly,
		logger:           l,
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
		s.enabled = c.Enabled
		s.room = c.Room
		s.token = c.Token
		s.url = c.URL
		s.global = c.Global
		s.stateChangesOnly = c.StateChangesOnly
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) Global() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.global
}

func (s *Service) StateChangesOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stateChangesOnly
}

func (s *Service) Alert(room, token, message string, level kapacitor.AlertLevel) error {
	url, post, err := s.preparePost(room, token, message, level)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand HipChat response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(room, token, message string, level kapacitor.AlertLevel) (string, io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.enabled {
		return "", nil, errors.New("service is not enabled")
	}
	//Generate HipChat API Url including room and authentication token
	if room == "" {
		room = s.room
	}
	if token == "" {
		token = s.token
	}

	var Url *url.URL
	Url, err := url.Parse(s.url + "/" + room + "/notification?auth_token=" + token)
	if err != nil {
		return "", nil, err
	}

	var color string
	switch level {
	case kapacitor.WarnAlert:
		color = "yellow"
	case kapacitor.CritAlert:
		color = "red"
	default:
		color = "green"
	}

	postData := make(map[string]interface{})
	postData["from"] = kapacitor.Product
	postData["color"] = color
	postData["message"] = message
	postData["notify"] = true

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}
	return Url.String(), &post, nil
}
