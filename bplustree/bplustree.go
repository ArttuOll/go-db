package bplustree

type Node struct {
	keys [][]byte
	// Only leaf nodes have values
	values [][]byte
	// Only internal nodes have children
	keyIds []*Node
}

func Encode(node *Node) []byte

func Decode(page []byte) (*Node, error)

type NodeType int

const (
	NODE NodeType = iota
	LEAF
)

const PAGE_SIZE = 4069
const MAX_KEY_SIZE = 1000
const MAX_VALUE_SIZE = 3000

func init() {
	node1max := 4 + 1*8 + 1*2 + 4 + MAX_KEY_SIZE + MAX_VALUE_SIZE
	if node1max > PAGE_SIZE {
		// The node can't fit on one page when serialized
		panic(1)
	}
}
