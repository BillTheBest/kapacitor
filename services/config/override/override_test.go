package override_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/kapacitor/services/config/override"
	"github.com/mitchellh/copystructure"
)

type SectionA struct {
	Option1 string `toml:"toml-option1" json:"json-option1"`
	Option2 string `toml:"toml-option2" json:"json-option2"`
}
type SectionB struct {
	Option3 string `toml:"toml-option3" json:"json-option3"`
}
type SectionC struct {
	Option4  int64  `toml:"toml-option4" json:"json-option4"`
	Password string `toml:"toml-password" json:"json-password" override:",redact"`
}
type SectionNums struct {
	Int   int
	Int8  int8
	Int16 int16
	Int32 int32
	Int64 int64

	Uint   uint
	Uint8  uint8
	Uint16 uint16
	Uint32 uint32
	Uint64 uint64

	Float32 float32
	Float64 float64
}

type TestConfig struct {
	SectionA    SectionA    `override:"section-a"`
	SectionB    SectionB    `override:"section-b"`
	SectionC    *SectionC   `override:"section-c"`
	SectionNums SectionNums `override:"section-nums"`
}

func ExampleOverrider() {
	config := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionB: SectionB{
			Option3: "o2",
		},
		SectionC: &SectionC{
			Option4: -1,
		},
	}

	// Create new ConfigOverrider
	cu := override.New(config)
	// Use toml tags to map field names
	cu.OptionNameFunc = override.TomlFieldName

	// Override options in section-a
	newSectionA, err := cu.Override("section-a", map[string]interface{}{
		"toml-option1": "new option1 value",
		"toml-option2": "initial option2 value",
	})
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	a := newSectionA.Value().(SectionA)
	fmt.Println("New SectionA.Option1:", a.Option1)
	fmt.Println("New SectionA.Option2:", a.Option2)

	// Override options in section-b
	newSectionB, err := cu.Override("section-b", map[string]interface{}{
		"toml-option3": "initial option3 value",
	})
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	b := newSectionB.Value().(SectionB)
	fmt.Println("New SectionB.Option3:", b.Option3)

	// Override options in section-c
	newSectionC, err := cu.Override("section-c", map[string]interface{}{
		"toml-option4": 586,
	})
	if err != nil {
		fmt.Println("ERROR:", err)
	}

	c := newSectionC.Value().(*SectionC)
	fmt.Println("New SectionC.Option4:", c.Option4)

	//Output:
	// New SectionA.Option1: new option1 value
	// New SectionA.Option2: initial option2 value
	// New SectionB.Option3: initial option3 value
	// New SectionC.Option4: 586
}

func TestOverrider_Override(t *testing.T) {
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionC: &SectionC{
			Option4: -1,
		},
	}
	copy, err := copystructure.Copy(testConfig)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		section        string
		set            map[string]interface{}
		exp            interface{}
		redacted       map[string]interface{}
		optionNameFunc override.OptionNameFunc
	}{
		{
			section: "section-a",
			set: map[string]interface{}{
				"Option1": "new-o1",
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"Option1": "new-o1",
				"Option2": "",
			},
		},
		{
			section:        "section-a",
			optionNameFunc: override.TomlFieldName,
			set: map[string]interface{}{
				"toml-option1": "new-o1",
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"toml-option1": "new-o1",
				"toml-option2": "",
			},
		},
		{
			section:        "section-a",
			optionNameFunc: override.JSONFieldName,
			set: map[string]interface{}{
				"json-option1": "new-o1",
			},
			exp: SectionA{
				Option1: "new-o1",
			},
			redacted: map[string]interface{}{
				"json-option1": "new-o1",
				"json-option2": "",
			},
		},
		{
			section:        "section-c",
			optionNameFunc: override.TomlFieldName,
			set: map[string]interface{}{
				"toml-option4": 42,
			},
			exp: &SectionC{
				Option4: 42,
			},
			redacted: map[string]interface{}{
				"toml-option4":  int64(42),
				"toml-password": false,
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int(42),
				"Int16":   int(42),
				"Int32":   int(42),
				"Int64":   int(42),
				"Uint":    int(42),
				"Uint8":   int(42),
				"Uint16":  int(42),
				"Uint32":  int(42),
				"Uint64":  int(42),
				"Float32": int(42),
				"Float64": int(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int(42),
				"Int16":   int(42),
				"Int32":   int(42),
				"Int64":   int(42),
				"Uint":    int(42),
				"Uint8":   int(42),
				"Uint16":  int(42),
				"Uint32":  int(42),
				"Uint64":  int(42),
				"Float32": int(42),
				"Float64": int(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int8(42),
				"Int8":    int8(42),
				"Int16":   int8(42),
				"Int32":   int8(42),
				"Int64":   int8(42),
				"Uint":    int8(42),
				"Uint8":   int8(42),
				"Uint16":  int8(42),
				"Uint32":  int8(42),
				"Uint64":  int8(42),
				"Float32": int8(42),
				"Float64": int8(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int16(42),
				"Int8":    int16(42),
				"Int16":   int16(42),
				"Int32":   int16(42),
				"Int64":   int16(42),
				"Uint":    int16(42),
				"Uint8":   int16(42),
				"Uint16":  int16(42),
				"Uint32":  int16(42),
				"Uint64":  int16(42),
				"Float32": int16(42),
				"Float64": int16(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int32(42),
				"Int8":    int32(42),
				"Int16":   int32(42),
				"Int32":   int32(42),
				"Int64":   int32(42),
				"Uint":    int32(42),
				"Uint8":   int32(42),
				"Uint16":  int32(42),
				"Uint32":  int32(42),
				"Uint64":  int32(42),
				"Float32": int32(42),
				"Float64": int32(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     int64(42),
				"Int8":    int64(42),
				"Int16":   int64(42),
				"Int32":   int64(42),
				"Int64":   int64(42),
				"Uint":    int64(42),
				"Uint8":   int64(42),
				"Uint16":  int64(42),
				"Uint32":  int64(42),
				"Uint64":  int64(42),
				"Float32": int64(42),
				"Float64": int64(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     uint(42),
				"Int8":    uint(42),
				"Int16":   uint(42),
				"Int32":   uint(42),
				"Int64":   uint(42),
				"Uint":    uint(42),
				"Uint8":   uint(42),
				"Uint16":  uint(42),
				"Uint32":  uint(42),
				"Uint64":  uint(42),
				"Float32": uint(42),
				"Float64": uint(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     uint8(42),
				"Int8":    uint8(42),
				"Int16":   uint8(42),
				"Int32":   uint8(42),
				"Int64":   uint8(42),
				"Uint":    uint8(42),
				"Uint8":   uint8(42),
				"Uint16":  uint8(42),
				"Uint32":  uint8(42),
				"Uint64":  uint8(42),
				"Float32": uint8(42),
				"Float64": uint8(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     uint16(42),
				"Int8":    uint16(42),
				"Int16":   uint16(42),
				"Int32":   uint16(42),
				"Int64":   uint16(42),
				"Uint":    uint16(42),
				"Uint8":   uint16(42),
				"Uint16":  uint16(42),
				"Uint32":  uint16(42),
				"Uint64":  uint16(42),
				"Float32": uint16(42),
				"Float64": uint16(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     uint32(42),
				"Int8":    uint32(42),
				"Int16":   uint32(42),
				"Int32":   uint32(42),
				"Int64":   uint32(42),
				"Uint":    uint32(42),
				"Uint8":   uint32(42),
				"Uint16":  uint32(42),
				"Uint32":  uint32(42),
				"Uint64":  uint32(42),
				"Float32": uint32(42),
				"Float64": uint32(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     uint64(42),
				"Int8":    uint64(42),
				"Int16":   uint64(42),
				"Int32":   uint64(42),
				"Int64":   uint64(42),
				"Uint":    uint64(42),
				"Uint8":   uint64(42),
				"Uint16":  uint64(42),
				"Uint32":  uint64(42),
				"Uint64":  uint64(42),
				"Float32": uint64(42),
				"Float64": uint64(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     float32(42),
				"Int8":    float32(42),
				"Int16":   float32(42),
				"Int32":   float32(42),
				"Int64":   float32(42),
				"Uint":    float32(42),
				"Uint8":   float32(42),
				"Uint16":  float32(42),
				"Uint32":  float32(42),
				"Uint64":  float32(42),
				"Float32": float32(42),
				"Float64": float32(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     float64(42),
				"Int8":    float64(42),
				"Int16":   float64(42),
				"Int32":   float64(42),
				"Int64":   float64(42),
				"Uint":    float64(42),
				"Uint8":   float64(42),
				"Uint16":  float64(42),
				"Uint32":  float64(42),
				"Uint64":  float64(42),
				"Float32": float64(42),
				"Float64": float64(42),
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-nums",
			set: map[string]interface{}{
				"Int":     "42",
				"Int8":    "42",
				"Int16":   "42",
				"Int32":   "42",
				"Int64":   "42",
				"Uint":    "42",
				"Uint8":   "42",
				"Uint16":  "42",
				"Uint32":  "42",
				"Uint64":  "42",
				"Float32": "42",
				"Float64": "42",
			},
			exp: SectionNums{
				Int:     int(42),
				Int8:    int8(42),
				Int16:   int16(42),
				Int32:   int32(42),
				Int64:   int64(42),
				Uint:    uint(42),
				Uint8:   uint8(42),
				Uint16:  uint16(42),
				Uint32:  uint32(42),
				Uint64:  uint64(42),
				Float32: float32(42),
				Float64: float64(42),
			},
			redacted: map[string]interface{}{
				"Int":     int(42),
				"Int8":    int8(42),
				"Int16":   int16(42),
				"Int32":   int32(42),
				"Int64":   int64(42),
				"Uint":    uint(42),
				"Uint8":   uint8(42),
				"Uint16":  uint16(42),
				"Uint32":  uint32(42),
				"Uint64":  uint64(42),
				"Float32": float32(42),
				"Float64": float64(42),
			},
		},
		{
			section: "section-c",
			set: map[string]interface{}{
				"Option4":  42,
				"Password": "supersecret",
			},
			exp: &SectionC{
				Option4:  int64(42),
				Password: "supersecret",
			},
			redacted: map[string]interface{}{
				"Option4":  int64(42),
				"Password": true,
			},
		},
	}
	for _, tc := range testCases {
		cu := override.New(testConfig)
		if tc.optionNameFunc != nil {
			cu.OptionNameFunc = tc.optionNameFunc
		}
		if newConfig, err := cu.Override(tc.section, tc.set); err != nil {
			t.Fatal(err)
		} else {
			// Validate value
			if got := newConfig.Value(); !reflect.DeepEqual(got, tc.exp) {
				t.Errorf("unexpected newConfig.Value result: got %v exp %v", got, tc.exp)
			}
			// Validate redacted
			if got, err := newConfig.Redacted(); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(got, tc.redacted) {
				t.Errorf("unexpected newConfig.Redacted result: got %v exp %v", got, tc.redacted)
			}
		}
		// Validate original not modified
		if !reflect.DeepEqual(testConfig, copy) {
			t.Errorf("original configuration object was modified. got %v exp %v", testConfig, copy)
		}
	}
}
