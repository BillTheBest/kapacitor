package sensu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"regexp"
	"sync"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	mu      sync.RWMutex
	enabled bool
	addr    string
	source  string
	logger  *log.Logger
}

var validNamePattern = regexp.MustCompile(`^[\w\.-]+$`)

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		enabled: c.Enabled,
		addr:    c.Addr,
		source:  c.Source,
		logger:  l,
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
		s.addr = c.Addr
		s.source = c.Source
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) Alert(name, output string, level kapacitor.AlertLevel) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	addr, postData, err := s.prepareData(name, output, level)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	err = enc.Encode(postData)
	if err != nil {
		return err
	}
	resp, err := ioutil.ReadAll(conn)
	if string(resp) != "ok" {
		return errors.New("sensu socket error: " + string(resp))
	}
	return nil
}

func (s *Service) prepareData(name, output string, level kapacitor.AlertLevel) (*net.TCPAddr, map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.enabled {
		return nil, nil, errors.New("service is not enabled")
	}

	var status int
	switch level {
	case kapacitor.OKAlert:
		status = 0
	case kapacitor.InfoAlert:
		status = 0
	case kapacitor.WarnAlert:
		status = 1
	case kapacitor.CritAlert:
		status = 2
	default:
		status = 3
	}

	postData := make(map[string]interface{})
	postData["name"] = name
	postData["source"] = s.source
	postData["output"] = output
	postData["status"] = status

	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		return nil, nil, err
	}

	return addr, postData, nil
}
