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

type GraphDiff struct {
	Added   map[string]*Node     `json:"added"`
	Removed map[string]*Node     `json:"removed"`
	Changed map[string]*NodeDiff `json:"changed"`
}

type NodeDiff struct {
	FromTitle string `json:"title"`
	ToTitle   string `json:"to_title"`

	// Keys are items, true means added, false means removed.
	Input  map[string]bool `json:"input"`
	Output map[string]bool `json:"output"`
}

func DiffSlice(a, b []string) map[string]bool {
	d := make(map[string]bool)

	ax := make(map[string]struct{}, len(a))
	for _, ai := range a {
		ax[ai] = struct{}{}
	}

	bx := make(map[string]struct{}, len(a))
	for _, bi := range b {
		bx[bi] = struct{}{}

		if _, ok := ax[bi]; !ok {
			d[bi] = true
		}
	}

	for _, ai := range a {
		if _, ok := bx[ai]; !ok {
			d[ai] = false
		}
	}

	if len(d) == 0 {
		return nil
	}

	return d
}

func DiffNode(a, b *Node) *NodeDiff {
	var d NodeDiff

	if a.Title != b.Title {
		d.FromTitle = a.Title
		d.ToTitle = b.Title
	}

	d.Input = DiffSlice(a.Input, b.Input)
	d.Output = DiffSlice(a.Output, b.Output)

	if d.FromTitle != "" || d.Input != nil || d.Output != nil {
		return &d
	}

	return nil
}

func DiffGraph(a, b *Graph) *GraphDiff {
	added := make(map[string]*Node)
	removed := make(map[string]*Node)
	changed := make(map[string]*NodeDiff)

	for ak, an := range a.Nodes {
		bn, ok := b.Nodes[ak]
		if !ok {
			removed[ak] = an
			continue
		}

		if nd := DiffNode(an, bn); nd != nil {
			changed[ak] = nd
		}
	}

	for bk, bn := range b.Nodes {
		if _, ok := a.Nodes[bk]; !ok {
			added[bk] = bn
		}
	}

	if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
		return &GraphDiff{
			Added:   added,
			Removed: removed,
			Changed: changed,
		}
	}

	return nil
}
