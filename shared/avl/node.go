package avl

type Node struct {
	Key    int64
	Value  []int
	Left   *Node
	Right  *Node
	Height int
}

func NewNode(k int64, v int) *Node {
	val := make([]int, 0, 1024)
	val = append(val, v)
	return &Node{
		Key:    k,
		Value:  val,
		Left:   nil,
		Right:  nil,
		Height: 1,
	}
}
