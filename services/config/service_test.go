package config_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httpd/httpdtest"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
)

type SectionA struct {
	Option1 string `override:"option-1"`
}

func (a SectionA) Validate() error {
	if a.Option1 == "invalid" {
		return errors.New("invalid option-1")
	}
	return nil
}

type SectionB struct {
	Option2  string `override:"option-2"`
	Password string `override:"password,redact"`
}

type SectionC struct {
	Name    string `override:"name"`
	Option3 int    `override:"option-3"`
}

type TestConfig struct {
	SectionA  SectionA   `override:"section-a"`
	SectionB  SectionB   `override:"section-b"`
	SectionCs []SectionC `override:"section-c,element-key=name"`
}

func OpenNewSerivce(testConfig interface{}, updates chan<- config.ConfigUpdate) (*config.Service, *httpdtest.Server) {
	c := config.NewConfig()
	service := config.NewService(c, testConfig, log.New(os.Stderr, "[config] ", log.LstdFlags), updates)
	service.StorageService = storagetest.New()
	server := httpdtest.NewServer(testing.Verbose())
	service.HTTPDService = server
	if err := service.Open(); err != nil {
		panic(err)
	}
	return service, server
}

func TestService_UpdateSection(t *testing.T) {
	testCases := []struct {
		body       string
		path       string
		expName    string
		expErr     error
		exp        interface{}
		updateErr  error
		skipUpdate bool
	}{
		// NOTE: These test cases all update the same underlying service,
		// so changes from one effect the next.
		// In other words the order of tests is important
		{
			body:       `{"set":{"option-1":"invalid"}}`,
			path:       "/section-a",
			expName:    "section-a",
			expErr:     errors.New("failed to override configuration section-a/: failed validation: invalid option-1"),
			skipUpdate: true, //error is validation error, so update is never sent
		},
		{
			body:    `{"set":{"option-1": "new-o1"}}`,
			path:    "/section-a",
			expName: "section-a",
			exp: []interface{}{
				SectionA{
					Option1: "new-o1",
				},
			},
		},
		{
			body:    `{"add":{"name":"element0","option-3": 7}}`,
			path:    "/section-c/",
			expName: "section-c",
			exp: []interface{}{
				SectionC{
					Name:    "element0",
					Option3: 7,
				},
				SectionC{
					Name:    "element1",
					Option3: 3,
				},
			},
		},
		{
			body:       `{"set":{"option-3": "bob"}}`,
			path:       "/section-c/element1",
			expName:    "section-c",
			expErr:     errors.New("failed to override configuration section-c/element1: cannot set option option-3: cannot convert string \"bob\" into int"),
			skipUpdate: true,
		},
		{
			body:    `{"delete":["option-1"]}`,
			path:    "/section-a",
			expName: "section-a",
			exp: []interface{}{
				SectionA{
					Option1: "o1",
				},
			},
		},
		{
			body:    `{"set":{"option-2":"valid"}}`,
			path:    "/section-b",
			expName: "section-b",
			expErr:  errors.New("failed to update configuration section-b/: failed to update service"),
			exp: []interface{}{
				SectionB{
					Option2: "valid",
				},
			},
			updateErr: errors.New("failed to update service"),
		},
		// Set unknown option
		{
			body:       `{"set":{"unknown": "value"}}`,
			path:       "/section-a",
			expName:    "section-a",
			expErr:     errors.New("failed to override configuration section-a/: unknown options [unknown] in section section-a"),
			skipUpdate: true,
		},
		// Validate unknown option was not persisted
		{
			body:    `{"set":{"option-1": "value"}}`,
			path:    "/section-a",
			expName: "section-a",
			exp: []interface{}{
				SectionA{
					Option1: "value",
				},
			},
		},
		// Try to add element to non list section
		{
			body:       `{"add":{"name":"element0","option-1": 7}}`,
			path:       "/section-a/",
			expName:    "section-a",
			expErr:     errors.New(`failed to apply update: section "section-a" is not a list, cannot add new element`),
			skipUpdate: true,
		},
	}
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionCs: []SectionC{
			{
				Name:    "element1",
				Option3: 3,
			},
		},
	}
	updates := make(chan config.ConfigUpdate, len(testCases))
	service, server := OpenNewSerivce(testConfig, updates)
	defer server.Close()
	defer service.Close()
	basePath := server.Server.URL + httpd.BasePath + "/config"
	for _, tc := range testCases {
		if !tc.skipUpdate {
			tc := tc
			go func() {
				cu := <-updates
				err := tc.updateErr
				if !reflect.DeepEqual(cu.NewConfig, tc.exp) {
					err = fmt.Errorf("unexpected new config: got %v exp %v", cu.NewConfig, tc.exp)
				}
				if got, exp := cu.Name, tc.expName; got != exp {
					err = fmt.Errorf("unexpected config update Name: got %s exp %s", got, exp)
				}
				cu.ErrC <- err
			}()
		}
		resp, err := http.Post(basePath+tc.path, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		// Validate response
		if tc.expErr != nil {
			gotErr := struct {
				Error string
			}{}
			json.Unmarshal(body, &gotErr)
			if got, exp := gotErr.Error, tc.expErr.Error(); got != exp {
				t.Fatalf("unexpected error:\ngot\n%q\nexp\n%q\n", got, exp)
			}
		} else if got, exp := resp.StatusCode, http.StatusNoContent; got != exp {
			t.Fatalf("unexpected code: got %d exp %d.\nBody:\n%s", got, exp, string(body))
		}

	}
}

func TestService_GetConfig(t *testing.T) {
	type update struct {
		Path string
		Body string
	}
	testCases := []struct {
		updates []update
		expName string
		exp     config.Sections
	}{
		{
			updates: []update{{
				Path: "/section-a",
				Body: `{"set":{"option-1": "new-o1"}}`,
			}},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "new-o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-a",
					Body: `{"delete":["option-1"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-b",
					Body: `{"set":{"option-2":"new-o2"},"delete":["option-nonexistant"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "new-o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "new-o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-a",
					Body: `{"set":{"option-1":"deleted"},"delete":["option-1"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-b",
					Body: `{"set":{"password": "secret"}}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": true,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c/x",
					Body: `{"set":{"option-3": 42}}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(42),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c/x",
					Body: `{"set":{"option-3": 42}}`,
				},
				{
					Path: "/section-c/x",
					Body: `{"delete":["option-3"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "w",
							"option-3": float64(42),
						},
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c",
					Body: `{"add":{"name":"q"}}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "q",
							"option-3": float64(0),
						},
						{
							"name":     "w",
							"option-3": float64(42),
						},
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c/w",
					Body: `{"set":{"option-3": 24}}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "w",
							"option-3": float64(24),
						},
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c/w",
					Body: `{"set":{"option-3": 24}}`,
				},
				{
					Path: "/section-c",
					Body: `{"remove":["w"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
		{
			// Only added overrides can be removed, not existing default elements
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"remove":["x"]}`,
				},
			},
			exp: config.Sections{
				"section-a": config.Section{
					Elements: []config.Element{{
						"option-1": "o1",
					}},
				},
				"section-b": config.Section{
					Elements: []config.Element{{
						"option-2": "o2",
						"password": false,
					}},
				},
				"section-c": config.Section{
					Elements: []config.Element{
						{
							"name":     "x",
							"option-3": float64(1),
						},
						{
							"name":     "y",
							"option-3": float64(2),
						},
						{
							"name":     "z",
							"option-3": float64(3),
						},
					},
				},
			},
		},
	}
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionB: SectionB{
			Option2: "o2",
		},
		SectionCs: []SectionC{
			{
				Name:    "x",
				Option3: 1,
			},
			{
				Name:    "y",
				Option3: 2,
			},
			{
				Name:    "z",
				Option3: 3,
			},
		},
	}
	for _, tc := range testCases {
		updates := make(chan config.ConfigUpdate, len(testCases))
		service, server := OpenNewSerivce(testConfig, updates)
		defer server.Close()
		defer service.Close()
		basePath := server.Server.URL + httpd.BasePath + "/config"
		// Apply all updates
		for _, update := range tc.updates {
			go func() {
				// Validate we got the update over the chan.
				// This keeps the chan unblocked.
				timer := time.NewTimer(10 * time.Millisecond)
				defer timer.Stop()
				select {
				case cu := <-updates:
					cu.ErrC <- nil
				case <-timer.C:
					t.Fatal("expected to get config update")
				}
			}()
			resp, err := http.Post(basePath+update.Path, "application/json", strings.NewReader(update.Body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := resp.StatusCode, http.StatusNoContent; got != exp {
				t.Fatalf("update failed: got: %d exp: %d\nBody:\n%s", got, exp, string(body))
			}
		}

		// Get config
		resp, err := http.Get(basePath)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("update failed: %d", resp.StatusCode)
		}

		got := make(config.Sections)
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, tc.exp) {
			t.Errorf("unexpected config:\ngot\n%v\nexp\n%v\n", got, tc.exp)
		}
	}
}
