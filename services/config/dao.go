package config

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrNoOverrideExists = errors.New("no override exists")
)

// Data access object for Override data.
type OverrideDAO interface {
	// Retrieve a override
	Get(id string) (Override, error)

	// Set an override.
	// If it does not already exist it will be created,
	// otherwise it will be replaced.
	Set(o Override) error

	// Delete a override.
	// It is not an error to delete an non-existent override.
	Delete(id string) error

	// List all overrides
	List() ([]Override, error)
}

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

type Override struct {
	// Unique identifier for the override
	ID string
	// Map of key value pairs of overrides.
	Overrides map[string]interface{}
}

const (
	overrideDataPrefix    = "/overrides/data/"
	overrideIndexesPrefix = "/overrides/indexes/"

	// Name of ID index
	idIndex = "id/"
)

// Key/Value store based implementation of the OverrideDAO
type overrideKV struct {
	store storage.Interface
}

func newOverrideKV(store storage.Interface) *overrideKV {
	return &overrideKV{
		store: store,
	}
}

func (d *overrideKV) encodeOverride(t Override) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(t)
	return buf.Bytes(), err
}

func (d *overrideKV) decodeOverride(data []byte) (Override, error) {
	var override Override
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&override)
	return override, err
}

// Create a key for the override data
func (d *overrideKV) overrideDataKey(id string) string {
	return overrideDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /overrides/data/ID -- contains encoded override data
// /overrides/index/id/ID -- contains the override ID
//
// As such to list all overrides in ID sorted order use the /overrides/index/id/ directory.
func (d *overrideKV) overrideIndexKey(index, value string) string {
	return overrideIndexesPrefix + index + value
}

func (d *overrideKV) Get(id string) (Override, error) {
	key := d.overrideDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return Override{}, err
	} else if !exists {
		return Override{}, ErrNoOverrideExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return Override{}, err
	}
	return d.decodeOverride(kv.Value)
}

func (d *overrideKV) Set(o Override) error {
	key := d.overrideDataKey(o.ID)

	data, err := d.encodeOverride(o)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	return nil
}

func (d *overrideKV) Delete(id string) error {
	key := d.overrideDataKey(id)
	indexKey := d.overrideIndexKey(idIndex, id)

	dataErr := d.store.Delete(key)
	indexErr := d.store.Delete(indexKey)
	if dataErr != nil {
		return dataErr
	}
	return indexErr
}

func (d *overrideKV) List() ([]Override, error) {
	// List all override ids sorted by ID
	ids, err := d.store.List(overrideIndexesPrefix + idIndex)
	if err != nil {
		return nil, err
	}
	overrides := make([]Override, len(ids))
	for i, id := range ids {
		var err error
		overrides[i], err = d.Get(id)
		if err != nil {
			return nil, err
		}
	}

	return overrides, nil
}
