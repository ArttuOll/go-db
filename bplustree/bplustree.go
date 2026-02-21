package bplustree

import "encoding/binary"

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

/*
The node format is:
| type | nkeys |  pointers  |  offsets   | key-values | unused |
|  2B  |   2B  | nkeys × 8B | nkeys × 2B |     ...    |        |

The offsets of each key-value pair are stored so that we don't
have to read all previous pairs to find a specific one.
*/
type Node []byte

func (node Node) getType() uint16 {
	// The first two bytes are the node type
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node Node) getNumberOfKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node Node) setHeader(nodeType uint16, numberOfKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], nodeType)
	binary.LittleEndian.PutUint16(node[2:4], numberOfKeys)
}

func (node Node) getPointer(index uint16) uint64 {
	if index >= node.getNumberOfKeys() {
		panic(1)
	}

	// Jump over the type and number of keys
	position := 4 + 8*index

	// Note that this method grabs only the first 8 bytes of the
	// byte array its given
	return binary.LittleEndian.Uint64(node[position:])
}

func (node Node) setPointer(index uint16, value uint64) {
	if index >= node.getNumberOfKeys() {
		panic(1)
	}

	position := 4 + 8*index
	binary.LittleEndian.PutUint64(node[position:], value)
}

func (node Node) getOffset(index uint16) uint16 {
	// The offset of the first key-value pair isn't stored
	// because it's just 0
	if index == 0 {
		return 0
	}

	// Jump over the type, number of keys, the pointers and
	// index - 1 offsets. Offsets are 2 bytes each.
	position := 4 + 8*node.getNumberOfKeys() + 2*(index-1)

	return binary.LittleEndian.Uint16(node[position:])
}

func (node Node) keyValuePosition(index uint16) uint16 {
	if index >= node.getNumberOfKeys() {
		panic(1)
	}

	return 4 + 8*node.getNumberOfKeys() + 2*node.getNumberOfKeys() + node.getOffset(index)
}

/*
The format of a key-value is.
| key_size | val_size | key | val |
|    2B    |    2B    | ... | ... |
*/
func (node Node) getKey(index uint16) []byte {
	if index >= node.getNumberOfKeys() {
		panic(1)
	}

	position := node.keyValuePosition(index)
	keySize := binary.LittleEndian.Uint16(node[position:])

	// Jump over the key size and value size
	return node[position+4:][:keySize]
}

func (node Node) getValue(index uint16) []byte {
	if index >= node.getNumberOfKeys() {
		panic(1)
	}

	position := node.keyValuePosition(index)
	keySize := binary.LittleEndian.Uint16(node[position:])

	// Jump over the key size to get the value size
	valueSize := binary.LittleEndian.Uint16(node[position+2:])

	return node[position+4+keySize:][:valueSize]
}
