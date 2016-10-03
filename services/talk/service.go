package talk

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
)

type Service struct {
	mu         sync.RWMutex
	enabled    bool
	url        string
	authorName string
	logger     *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		enabled:    c.Enabled,
		url:        c.URL,
		authorName: c.AuthorName,
		logger:     l,
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
		s.url = c.URL
		s.authorName = c.AuthorName
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) Alert(title, text string) error {
	url, post, err := s.preparePost(title, text)
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
		r := &response{Error: fmt.Sprintf("failed to understand Talk response. code: %d content: %s", resp.StatusCode, string(body))}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(title, text string) (string, io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.enabled {
		return "", nil, errors.New("service is not enabled")
	}
	postData := make(map[string]interface{})
	postData["title"] = title
	postData["text"] = text
	postData["authorName"] = s.authorName

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return s.url, &post, nil
}
