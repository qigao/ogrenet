package passthru

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBodyCodec(t *testing.T) {
	t.Run("encode/decode sample data by json", func(t *testing.T) {
		m := make(map[string]interface{})
		m["name"] = "qigao"
		m["age"] = "18"

		data, err := json.Marshal(m)
		assert.Nil(t, err)
		result := make(map[string]interface{})
		err = json.Unmarshal(data, &result)
		assert.Nil(t, err)
		assert.Equal(t, m, result)
	})
}

type Person struct {
	Name  string
	Age   int
	Other map[string]interface{} `mapstructure:",remain"`
}

func BenchmarkMarshalJson(b *testing.B) {
	input := map[string]interface{}{
		"name":  "Mitchell",
		"age":   91,
		"email": "mitchell@gmail.com",
	}

	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(input)
	}
}

func BenchmarkUnMarshalJson(b *testing.B) {
	input := `{
		"name":  "Mitchell",
		"age":   91,
		"email": "mitchell@gmail.com"}`
	for i := 0; i < b.N; i++ {
		var result Person
		_ = json.Unmarshal([]byte(input), &result)
	}
}

func BenchmarkPerson(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := &Person{
			Name: "Mitchell",
			Age:  91,
			Other: map[string]interface{}{
				"email": "mitchell@gmail.com",
			},
		}
		_, _ = json.Marshal(p)
	}
}

func BenchmarkUnmarshallPerson(b *testing.B) {
	input := `{
		"name":  "Mitchell",
		"age":   91,
		"email": "mitchel@gmail.com"}`
	for i := 0; i < b.N; i++ {
		var result Person
		_ = json.Unmarshal([]byte(input), &result)
	}
}
