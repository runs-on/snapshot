package utils

import (
	"encoding/json"
	"fmt"
)

func PrettyPrint(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", v) // fallback
	}
	return string(b)
}
