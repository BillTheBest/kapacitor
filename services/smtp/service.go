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
	mu      sync.RWMutex
	c       Config
	mail    chan update
	running bool
	logger  *log.Logger
	wg      sync.WaitGroup
}

type update struct {
	newDialer bool
	message   *gomail.Message
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		c:      c,
		mail:   make(chan update),
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
		s.mu.Lock()
		previousEnabled := s.c.Enabled
		s.c = c
		// If we have not already started the runMailer goroutine start it now.
		if !s.running && !previousEnabled && s.c.Enabled {
			s.wg.Add(1)
			go s.runMailer()
			s.running = true
		}
		s.mu.Unlock()
		if c.Enabled {
			// Signal to create new dialer
			s.mail <- update{newDialer: true}
		}
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

func (s *Service) dialer() (d *gomail.Dialer, idleTimeout time.Duration) {
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
	idleTimeout = time.Duration(s.c.IdleTimeout)
	return
}

func (s *Service) runMailer() {
	defer s.wg.Done()

	var idleTimeout time.Duration
	var d *gomail.Dialer
	d, idleTimeout = s.dialer()

	var conn gomail.SendCloser
	var err error
	open := false
	for {
		timer := time.NewTimer(idleTimeout)
		select {
		case u, ok := <-s.mail:
			if !ok {
				return
			}
			// Check for special nil message to update dialer
			if u.newDialer {
				// Close old connection
				if conn != nil {
					if err := conn.Close(); err != nil {
						s.logger.Println("E! error closing connection to old SMTP server:", err)
					}
					conn = nil
				}
				// Create new dialer
				d, idleTimeout = s.dialer()
				open = false
				// Nothing more to do
				break
			}
			if !open {
				if conn, err = d.Dial(); err != nil {
					s.logger.Println("E! error connecting to SMTP server", err)
					break
				}
				open = true
			}
			if err := gomail.Send(conn, u.message); err != nil {
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
	s.mail <- update{message: m}
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
