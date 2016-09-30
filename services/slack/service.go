package slack

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

	"github.com/influxdata/kapacitor"
)

type Service struct {
	mu               sync.RWMutex
	channel          string
	url              string
	global           bool
	stateChangesOnly bool
	logger           *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		channel:          c.Channel,
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
		s.channel = c.Channel
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

// slack attachment info
type attachment struct {
	Fallback string `json:"fallback"`
	Color    string `json:"color"`
	Text     string `json:"text"`
}

func (s *Service) Alert(channel, message string, level kapacitor.AlertLevel) error {
	url, post, err := s.preparePost(channel, message, level)
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
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Slack response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(channel, message string, level kapacitor.AlertLevel) (string, io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if channel == "" {
		channel = s.channel
	}
	var color string
	switch level {
	case kapacitor.WarnAlert:
		color = "warning"
	case kapacitor.CritAlert:
		color = "danger"
	default:
		color = "good"
	}
	a := attachment{
		Fallback: message,
		Text:     message,
		Color:    color,
	}
	postData := make(map[string]interface{})
	postData["channel"] = channel
	postData["username"] = kapacitor.Product
	postData["text"] = ""
	postData["attachments"] = []attachment{a}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return s.url, &post, nil
}
