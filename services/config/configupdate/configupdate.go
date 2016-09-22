package configupdate

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/mitchellh/copystructure"
	"github.com/mitchellh/reflectwalk"
	"github.com/pkg/errors"
)

const (
	structTagKey = "configupdate"
)

// FieldNameFunc returns the name of a field based on its
// reflect.StructField description.
type FieldNameFunc func(reflect.StructField) string

type ConfigUpdater struct {
	// original is the original configuration value provided
	// It is not modified, only copies will be modified.
	original interface{}
	// FieldNameFunc is responsible for determining the names of struct fields.
	FieldNameFunc FieldNameFunc
}

// New ConfigUpdater that will update the given configuration interface.
func New(config interface{}) *ConfigUpdater {
	return &ConfigUpdater{
		original:      config,
		FieldNameFunc: RawFieldName,
	}
}

// Update a section with values from the set
func (c *ConfigUpdater) Update(section, name string, set map[string]interface{}) (interface{}, error) {
	if section == "" {
		return nil, errors.New("section cannot be empty")
	}
	// First make a copy into which we can apply the updates.
	copy, err := copystructure.Copy(c.original)
	if err != nil {
		return nil, errors.Wrap(err, "failed to copy configuration object")
	}
	walker := newWalker(
		section,
		name,
		set,
		c.FieldNameFunc,
	)

	// walk the copy add apply the updates
	if err := reflectwalk.Walk(copy, walker); err != nil {
		return nil, errors.Wrapf(err, "failed to apply changes to configuration object for section %s", section)
	}
	unused := walker.unused()
	if len(unused) > 0 {
		return nil, fmt.Errorf("unknown options %v in sectoin %s", unused, section)
	}
	// Return the modified copy
	newValue := walker.sectionObject()
	if newValue == nil {
		return nil, fmt.Errorf("unknown section %s", section)
	}
	return newValue, nil
}

// TomlFieldName returns the name of a field based on its "toml" struct tag.
func TomlFieldName(f reflect.StructField) (name string) {
	return tagFieldName("toml", f)
}

// JSONFieldName returns the name of a field based on its "json" struct tag.
func JSONFieldName(f reflect.StructField) (name string) {
	return tagFieldName("json", f)
}

// RawFieldName returns the name of a field based on its Go field name.
func RawFieldName(f reflect.StructField) (name string) {
	return f.Name
}

// tagFieldName returns the name of a field based on the value of a given struct tag.
// All content after a "," is ignored.
func tagFieldName(tag string, f reflect.StructField) (name string) {
	parts := strings.Split(f.Tag.Get(tag), ",")
	if len(parts) > 0 {
		name = parts[0]
	}
	if name == "" {
		name = f.Name
	}
	return
}

// configWalker applys the changes onto the walked value.
type configWalker struct {
	section string
	name    string
	set     map[string]interface{}
	used    map[string]bool

	sectionFieldName string
	sectionValue     reflect.Value

	fieldNameFunc   func(reflect.StructField) string
	currentFieldTag string
	depth           int
}

func newWalker(section, name string, set map[string]interface{}, fieldNameFunc FieldNameFunc) *configWalker {
	return &configWalker{
		section:       section,
		name:          name,
		set:           set,
		used:          make(map[string]bool, len(set)),
		fieldNameFunc: fieldNameFunc,
	}
}

func (w *configWalker) unused() []string {
	unused := make([]string, 0, len(w.set))
	for name := range w.set {
		if !w.used[name] {
			unused = append(unused, name)
		}
	}
	return unused
}
func (w *configWalker) sectionObject() interface{} {
	if w.sectionValue.IsValid() {
		return w.sectionValue.Interface()
	}
	return nil
}

func (w *configWalker) Enter(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth++
	}
	return nil
}

func (w *configWalker) Exit(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth--
	}
	return nil
}

func (w *configWalker) Struct(reflect.Value) error {
	return nil
}

func (w *configWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Section level
	case 0:
		parts := strings.Split(f.Tag.Get(structTagKey), ",")
		if len(parts) > 0 {
			w.currentFieldTag = parts[0]
			if w.section == w.currentFieldTag {
				w.sectionFieldName = f.Name
				w.sectionValue = v
			}
		}
	// Option level
	case 1:
		// Skip this field if its not for the section we care about
		if w.currentFieldTag != w.section {
			break
		}
		name := w.fieldNameFunc(f)
		setValue, ok := w.set[name]
		if ok {
			if err := weakCopyValue(reflect.ValueOf(setValue), v); err != nil {
				return errors.Wrapf(err, "cannot set option %s", name)
			}
			w.used[name] = true
		}
	}
	return nil
}

// weakCopyValue copies the value of dst into src, where numeric types are copied weakly.
func weakCopyValue(src, dst reflect.Value) error {
	if !dst.CanSet() {
		return errors.New("not settable")
	}
	srcK := src.Kind()
	dstK := dst.Kind()
	if srcK == dstK {
		// Perform normal copy
		dst.Set(src)
		return nil
	} else if isNumericKind(dstK) {
		// Perform weak numeric copy
		switch {
		case isIntKind(dstK):
			switch {
			case isIntKind(srcK):
				dst.SetInt(src.Int())
				return nil
			case isUintKind(srcK):
				dst.SetInt(int64(src.Uint()))
				return nil
			case isFloatKind(srcK):
				dst.SetInt(int64(src.Float()))
				return nil
			case srcK == reflect.String:
				if i, err := strconv.ParseInt(src.String(), 10, 64); err == nil {
					dst.SetInt(i)
					return nil
				}
			}
		case isUintKind(dstK):
			switch {
			case isIntKind(srcK):
				dst.SetUint(uint64(src.Int()))
				return nil
			case isUintKind(srcK):
				dst.SetUint(src.Uint())
				return nil
			case isFloatKind(srcK):
				dst.SetUint(uint64(src.Float()))
				return nil
			case srcK == reflect.String:
				if i, err := strconv.ParseUint(src.String(), 10, 64); err == nil {
					dst.SetUint(i)
					return nil
				}
			}
		case isFloatKind(dstK):
			switch {
			case isIntKind(srcK):
				dst.SetFloat(float64(src.Int()))
				return nil
			case isUintKind(srcK):
				dst.SetFloat(float64(src.Uint()))
				return nil
			case isFloatKind(srcK):
				dst.SetFloat(src.Float())
				return nil
			case srcK == reflect.String:
				if f, err := strconv.ParseFloat(src.String(), 64); err == nil {
					dst.SetFloat(f)
					return nil
				}
			}
		}
	}
	return fmt.Errorf("wrong type %s, expected value of type %s", srcK, dstK)
}

func isNumericKind(k reflect.Kind) bool {
	// Ignoring complex kinds since we cannot convert them
	return k >= reflect.Int && k <= reflect.Float64
}
func isIntKind(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}
func isUintKind(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uint64
}
func isFloatKind(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}
