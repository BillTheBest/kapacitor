// Overrider provides an API for overriding and reading redacted values for a configuration object.
// The configuration object provided is expected to have two levels of nested structs.
// The "section" level represents the top level fields of the object.
// The "option" level represents the second level of fields in the object.
// Further levels may exist but Overrider will not interact with them.
//
// In order for a section to be overridden an `override` struct tag must be present.
// The `override` tag defines a name for the section.
// Struct tags can be used to mark fields as redacted as well, by adding a `,redact` to the end of the tag value.
// Overrider also has support for reading option names from custom struct tags like `toml` or `json`
// via the OptionNameFunc field of the Overrider type.
//
// Example:
//    type MySectionConfig struct {
//        Option   string `toml:"option"`
//        Password string `toml:"password" override:",redact"`
//    }
//    type MyConfig struct {
//        SectionName    MySectionConfig      `override:"section-name"`
//        IgnoredSection MyOtherSectionConfig
//    }
//    type MyOtherSectionConfig struct{}
//    // Setup
//    c := MyConfig{
//        MySectionConfig: MySectionConfig{
//             Option: "option value",
//             Password: "secret",
//        },
//        IgnoredSection: MyOtherSectionConfig{},
//    }
//    o := override.New(c)
//    o.OptionNameFunc = override.TomlFieldName
//    // Read redacted section values
//    redacted, err := o.Sections()
//    // Override options for a section
//    newSection, err := o.Override("section-name", map[string]interface{}{"option": "overridden option value"})
package override

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
	structTagKey  = "override"
	redactKeyword = "redact"
)

// OptionNameFunc returns the name of a field based on its
// reflect.StructField description.
type OptionNameFunc func(reflect.StructField) string

// TomlFieldName returns the name of a field based on its "toml" struct tag.
// If the tag is absent the Go field name is used.
func TomlFieldName(f reflect.StructField) (name string) {
	return tagFieldName("toml", f)
}

// JSONFieldName returns the name of a field based on its "json" struct tag.
// If the tag is absent the Go field name is used.
func JSONFieldName(f reflect.StructField) (name string) {
	return tagFieldName("json", f)
}

// OverrideFieldName returns the name of a field based on its "override" struct tag.
// If the tag is absent the Go field name is used.
func OverrideFieldName(f reflect.StructField) (name string) {
	return tagFieldName(structTagKey, f)
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

// Overrider provides methods for overriding and reading redacted values for a configuration object.
type Overrider struct {
	// original is the original configuration value provided
	// It is not modified, only copies will be modified.
	original interface{}
	// OptionNameFunc is responsible for determining the names of struct fields.
	OptionNameFunc OptionNameFunc
}

// New Overrider where the config object is expected to have two levels of nested structs.
func New(config interface{}) *Overrider {
	return &Overrider{
		original:       config,
		OptionNameFunc: OverrideFieldName,
	}
}

// Override a section with values from the set of overrides.
// The overrides is a map of option name to value.
//
// Values must be of the same type as the named option field, with the exception of numeric types.
// Numeric types will be converted to the absolute type using Go's default conversion mechanisms.
// Strings will also be parsed for numeric values if needed.
// Any mismatched type or failure to convert to a numeric type will result in an error.
//
// The underlying configuration object is not modified, but rather a copy is returned via the Section type.
func (c *Overrider) Override(section string, overrides map[string]interface{}) (Section, error) {
	if section == "" {
		return Section{}, errors.New("section cannot be empty")
	}
	// First make a copy into which we can apply the updates.
	copy, err := copystructure.Copy(c.original)
	if err != nil {
		return Section{}, errors.Wrap(err, "failed to copy configuration object")
	}
	walker := newOverrideWalker(
		section,
		overrides,
		c.OptionNameFunc,
	)

	// walk the copy and apply the updates
	if err := reflectwalk.Walk(copy, walker); err != nil {
		return Section{}, errors.Wrapf(err, "failed to apply changes to configuration object for section %s", section)
	}
	unused := walker.unused()
	if len(unused) > 0 {
		return Section{}, fmt.Errorf("unknown options %v in section %s", unused, section)
	}
	// Return the modified copy
	newValue := walker.sectionObject()
	if newValue == nil {
		return Section{}, fmt.Errorf("unknown section %s", section)
	}
	return Section{section: newValue, optionNameFunc: c.OptionNameFunc}, nil
}

// Sections returns the original unmodified configuration sections.
func (c *Overrider) Sections() (map[string]Section, error) {
	// walk the original and read all sections
	walker := newSectionWalker(c.OptionNameFunc)
	if err := reflectwalk.Walk(c.original, walker); err != nil {
		return nil, errors.Wrap(err, "failed to read sections from configuration object")
	}

	return walker.sectionsMap(), nil
}

// overrideWalker applies the changes onto the walked value.
type overrideWalker struct {
	depthWalker
	section string
	set     map[string]interface{}
	used    map[string]bool

	sectionValue reflect.Value

	optionNameFunc     OptionNameFunc
	currentSectionName string
}

func newOverrideWalker(section string, set map[string]interface{}, optionNameFunc OptionNameFunc) *overrideWalker {
	return &overrideWalker{
		section:        section,
		set:            set,
		used:           make(map[string]bool, len(set)),
		optionNameFunc: optionNameFunc,
	}
}

func (w *overrideWalker) unused() []string {
	unused := make([]string, 0, len(w.set))
	for name := range w.set {
		if !w.used[name] {
			unused = append(unused, name)
		}
	}
	return unused
}

func (w *overrideWalker) sectionObject() interface{} {
	if w.sectionValue.IsValid() {
		return w.sectionValue.Interface()
	}
	return nil
}

func (w *overrideWalker) Struct(reflect.Value) error {
	return nil
}

func (w *overrideWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Section level
	case 0:
		name, ok := getSectionName(f)
		if ok {
			// Only override the section if a struct tag was present
			w.currentSectionName = name
			if w.section == w.currentSectionName {
				w.sectionValue = v
			}
		}
	// Option level
	case 1:
		// Skip this field if its not for the section we care about
		if w.currentSectionName != w.section {
			break
		}
		name := w.optionNameFunc(f)
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
		if isNumericKind(srcK) {
			dst.Set(src.Convert(dst.Type()))
			return nil
		}
		// Check for string kind
		if srcK == reflect.String {
			switch {
			case isIntKind(dstK):
				if i, err := strconv.ParseInt(src.String(), 10, 64); err == nil {
					dst.SetInt(i)
					return nil
				}
			case isUintKind(dstK):
				if i, err := strconv.ParseUint(src.String(), 10, 64); err == nil {
					dst.SetUint(i)
					return nil
				}
			case isFloatKind(dstK):
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

// Section provides access to the underlying value or a map of redacted values.
type Section struct {
	section        interface{}
	optionNameFunc OptionNameFunc
}

// Value returns the underlying value.
func (s Section) Value() interface{} {
	return s.section
}

// Redacted returns the options for the section in a map.
// Any fields with the `override:",redact"` tag set will be replaced
// with a boolean value indicating whether a non-zero value was set.
func (s Section) Redacted() (map[string]interface{}, error) {
	walker := newRedactWalker(s.optionNameFunc)
	// walk the section and collect redacted options
	if err := reflectwalk.Walk(s.section, walker); err != nil {
		return nil, errors.Wrap(err, "failed to redact section")
	}
	return walker.optionsMap(), nil
}

// redactWalker reads the the sections from the walked values and redacts and sensitive fields.
type redactWalker struct {
	depthWalker
	options        map[string]interface{}
	optionNameFunc OptionNameFunc
}

func newRedactWalker(optionNameFunc OptionNameFunc) *redactWalker {
	if optionNameFunc == nil {
		optionNameFunc = OverrideFieldName
	}
	return &redactWalker{
		options:        make(map[string]interface{}),
		optionNameFunc: optionNameFunc,
	}
}

func (w *redactWalker) optionsMap() map[string]interface{} {
	return w.options
}

func (w *redactWalker) Struct(reflect.Value) error {
	return nil
}

func (w *redactWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	case 0:
		// Top level
		name := w.optionNameFunc(f)
		w.options[name] = getRedactedValue(f, v)
	default:
		// Ignore all other levels
	}
	return nil
}

func getRedactedValue(f reflect.StructField, v reflect.Value) interface{} {
	if isRedacted(f) {
		return !isZero(v)
	} else {
		return v.Interface()
	}
}

func isRedacted(f reflect.StructField) bool {
	parts := strings.Split(f.Tag.Get(structTagKey), ",")
	if len(parts) > 1 {
		for _, p := range parts[1:] {
			if p == redactKeyword {
				return true
			}
		}
	}
	return false
}

// isZero returns whether if v is equal to the zero value of its type.
func isZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Func, reflect.Map, reflect.Slice:
		return v.IsNil()
	case reflect.Array:
		// Check arrays linearly since its element type may not be comparable.
		z := true
		for i := 0; i < v.Len() && z; i++ {
			z = z && isZero(v.Index(i))
		}
		return z
	case reflect.Struct:
		// Check structs recusively since not all of its field may be comparable
		z := true
		for i := 0; i < v.NumField() && z; i++ {
			if f := v.Field(i); f.CanSet() {
				z = z && isZero(f)
			}
		}
		return z
	default:
		// Compare other types directly:
		z := reflect.Zero(v.Type())
		return v.Interface() == z.Interface()
	}
}

// sectionWalker reads the sections from the walked values and redacts any sensitive fields.
type sectionWalker struct {
	depthWalker
	sections       map[string]Section
	optionNameFunc OptionNameFunc
}

func newSectionWalker(optionNameFunc OptionNameFunc) *sectionWalker {
	return &sectionWalker{
		sections:       make(map[string]Section),
		optionNameFunc: optionNameFunc,
	}
}

func (w *sectionWalker) sectionsMap() map[string]Section {
	return w.sections
}

func (w *sectionWalker) Struct(reflect.Value) error {
	return nil
}

func (w *sectionWalker) StructField(f reflect.StructField, v reflect.Value) error {
	switch w.depth {
	// Section level
	case 0:
		name, ok := getSectionName(f)
		if ok {
			w.sections[name] = Section{
				section:        v.Interface(),
				optionNameFunc: w.optionNameFunc,
			}
		}
	// Skip all other levels
	default:
	}
	return nil
}

// getSectionName returns the name of the section based off its `override` struct tag.
// If no tag is present the Go field name is returned and the second return value is false.
func getSectionName(f reflect.StructField) (string, bool) {
	parts := strings.Split(f.Tag.Get(structTagKey), ",")
	if len(parts) > 0 {
		return parts[0], true
	}
	return f.Name, false
}

// depthWalker keeps track of the current depth count into nested structs.
type depthWalker struct {
	depth int
}

func (w *depthWalker) Enter(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth++
	}
	return nil
}

func (w *depthWalker) Exit(l reflectwalk.Location) error {
	if l == reflectwalk.StructField {
		w.depth--
	}
	return nil
}
