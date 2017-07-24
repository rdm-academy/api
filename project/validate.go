package project

import (
	"fmt"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

type Errors []error

func (es Errors) Error() string {
	l := make([]string, len(es))

	for i, e := range es {
		l[i] = fmt.Sprintf("- %s", e.Error())
	}

	return strings.Join(l, "\n")
}

type Graph struct {
	Nodes map[string]*Node
	// Map of edges from source to dest.
	Out map[string][]*Node
	// Map of edges from dst to source.
	In map[string][]*Node
}

// TODO: Copy validation logic on client-side.
func CheckGraph() error {
	return nil
}

func ParseSource(source string) (*Graph, error) {
	var g Graph
	if err := yaml.Unmarshal([]byte(source), &g.Nodes); err != nil {
		return nil, err
	}

	return &g, nil
}
