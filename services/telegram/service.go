package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

type Service struct {
	mu                    sync.RWMutex
	chatId                string
	parseMode             string
	disableWebPagePreview bool
	disableNotification   bool
	url                   string
	global                bool
	stateChangesOnly      bool
	logger                *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		chatId:                c.ChatId,
		parseMode:             c.ParseMode,
		disableWebPagePreview: c.DisableWebPagePreview,
		disableNotification:   c.DisableNotification,
		url:                   c.URL + c.Token + "/sendMessage",
		global:                c.Global,
		stateChangesOnly:      c.StateChangesOnly,
		logger:                l,
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
		s.chatId = c.ChatId
		s.parseMode = c.ParseMode
		s.disableWebPagePreview = c.DisableWebPagePreview
		s.disableNotification = c.DisableNotification
		s.url = c.URL + c.Token + "/sendMessage"
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

func (s *Service) Alert(chatId, parseMode, message string, disableWebPagePreview, disableNotification bool) error {
	url, post, err := s.preparePost(chatId, parseMode, message, disableWebPagePreview, disableNotification)
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
			Description string `json:"description"`
			ErrorCode   int    `json:"error_code"`
			Ok          bool   `json:"ok"`
		}
		res := &response{}

		err = json.Unmarshal(body, res)

		if err != nil {
			return fmt.Errorf("failed to understand Telegram response (err: %s). code: %d content: %s", err.Error(), resp.StatusCode, string(body))
		}
		return fmt.Errorf("sendMessage error (%d) description: %s", res.ErrorCode, res.Description)

	}
	return nil
}
func (s *Service) preparePost(chatId, parseMode, message string, disableWebPagePreview, disableNotification bool) (string, io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if chatId == "" {
		chatId = s.chatId
	}

	if parseMode == "" {
		parseMode = s.parseMode
	}

	if parseMode != "" && parseMode != "Markdown" && parseMode != "HTML" {
		return "", nil, fmt.Errorf("parseMode %s is not valid, please use 'Markdown' or 'HTML'", parseMode)
	}

	postData := make(map[string]interface{})
	postData["chat_id"] = chatId
	postData["text"] = message

	if parseMode != "" {
		postData["parse_mode"] = parseMode
	}

	if disableWebPagePreview || s.disableWebPagePreview {
		postData["disable_web_page_preview"] = true
	}

	if disableNotification || s.disableNotification {
		postData["disable_notification"] = true
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return s.url, &post, nil
}
