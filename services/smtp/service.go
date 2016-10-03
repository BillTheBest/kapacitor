package smtp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"gopkg.in/gomail.v2"
)

var ErrNoRecipients = errors.New("not sending email, no recipients defined")

type Service struct {
	mu     sync.RWMutex
	c      Config
	mail   chan *gomail.Message
	logger *log.Logger
	wg     sync.WaitGroup
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		c:      c,
		mail:   make(chan *gomail.Message),
		logger: l,
	}
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Println("I! Starting SMTP service")

	if !s.c.Enabled {
		return nil
	}

	s.wg.Add(1)
	go s.runMailer()
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Println("I! Closing SMTP service")
	close(s.mail)
	s.wg.Wait()
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		nowEnabled := false
		s.mu.Lock()
		nowEnabled = !s.c.Enabled && c.Enabled
		s.c = c
		s.mu.Unlock()
		if nowEnabled {
			if c.From == "" {
				return errors.New("cannot open smtp service: missing from address in configuration")
			}
			s.wg.Add(1)
			go s.runMailer()
		}
		// Signal to create new dialer
		s.mail <- nil
	}
	return nil
}

func (s *Service) Global() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.c.Global
}

func (s *Service) StateChangesOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.c.StateChangesOnly
}

func (s *Service) dialer() (d *gomail.Dialer) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.c.Username == "" {
		d = &gomail.Dialer{Host: s.c.Host, Port: s.c.Port}
	} else {
		d = gomail.NewPlainDialer(s.c.Host, s.c.Port, s.c.Username, s.c.Password)
	}
	if s.c.NoVerify {
		d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return
}

func (s *Service) runMailer() {
	defer s.wg.Done()

	d := s.dialer()
	idleTimeout := time.Duration(s.c.IdleTimeout)

	var conn gomail.SendCloser
	var err error
	open := false
	for {
		timer := time.NewTimer(idleTimeout)
		select {
		case m, ok := <-s.mail:
			if !ok {
				return
			}
			// Check for special nil message to update dialer
			if m == nil {
				// Close old connection
				if conn != nil {
					if err := conn.Close(); err != nil {
						s.logger.Println("E! error closing connection to old SMTP server:", err)
					}
				}
				// Create new dialer
				d = s.dialer()
				open = false
				// Update idleTimeout
				s.mu.RLock()
				idleTimeout = time.Duration(s.c.IdleTimeout)
				s.mu.RUnlock()
				// Nothing more to do with nil message
				break
			}
			if !open {
				if conn, err = d.Dial(); err != nil {
					s.logger.Println("E! error connecting to SMTP server", err)
					break
				}
				open = true
			}
			log.Println("D! conn", conn, open)
			if err := gomail.Send(conn, m); err != nil {
				s.logger.Println("E!", err)
			}
		// Close the connection to the SMTP server if no email was sent in
		// the last IdleTimeout duration.
		case <-timer.C:
			if open {
				if err := conn.Close(); err != nil {
					s.logger.Println("E! error closing connection to SMTP server:", err)
				}
				open = false
			}
		}
		timer.Stop()
	}
}

func (s *Service) SendMail(to []string, subject, body string) error {
	m, err := s.prepareMessge(to, subject, body)
	if err != nil {
		return err
	}
	s.mail <- m
	return nil
}

func (s *Service) prepareMessge(to []string, subject, body string) (*gomail.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.c.Enabled {
		return nil, errors.New("service not enabled")
	}
	if len(to) == 0 {
		to = s.c.To
	}
	if len(to) == 0 {
		return nil, ErrNoRecipients
	}
	m := gomail.NewMessage()
	m.SetHeader("From", s.c.From)
	m.SetHeader("To", to...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)
	return m, nil
}
