package avl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAVLTree(t *testing.T) {
	testSlice := map[int64]int{
		124365434: 1,
		124365435: 2,
		124365436: 3,
		124365437: 4,
		124365438: 5,
		124365439: 6,
		124365440: 7,
		124365441: 8,
		124365442: 9,
		124365443: 10,
		124365444: 11,
	}

	avlTree := NewAvlTree()

	for k, v := range testSlice {
		avlTree.Add(k, v)
	}

	assert.True(t, avlTree.IsBST())
	fmt.Println("root ====", avlTree.GetRoot(), " inorder keys===", avlTree.InOrder(-1))
	_ = avlTree.Set(124365434, 124365444, 1)
	_ = avlTree.Set(124365435, 124365444, 2)
	_ = avlTree.Set(124365436, 124365444, 3)
	_ = avlTree.Set(124365437, 124365444, 4)
	_ = avlTree.Set(124365438, 124365444, 5)
	_ = avlTree.Set(124365439, 124365444, 6)
	_ = avlTree.Set(124365444, 124365445, 1)
	_ = avlTree.Set(124365444, 124365445, 2)
	_ = avlTree.Set(124365444, 124365445, 3)
	_ = avlTree.Set(124365444, 124365445, 4)
	_ = avlTree.Set(124365444, 124365445, 5)
	_ = avlTree.Set(124365444, 124365445, 6)
	_ = avlTree.Set(124365444, 124365445, 11)
	avlTree.Remove(124365440)
	fmt.Println("root ====", avlTree.GetRoot(), " inorder keys===", avlTree.InOrder(-1))
	//sl := []int{}
	//for _, v := range avlTree.InOrder(-1) {
	//	sl = append(sl, avlTree.Get(v)...)
	//}
	//fmt.Println(sl)
	fmt.Println(avlTree.GetLessThanKey(124365444))
}
