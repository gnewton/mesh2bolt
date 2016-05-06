package main

import (
	"log"
	"strings"
)

type Node struct {
	TreeNumber string
	NodeLabel  string
	RecordId   string
	Name       string
	ChildIds   []string
	ChildNodes []*Node
}

func (node *Node) AddNode(treeNumber string, descriptorUi string, descriptorName string) {
	parts := strings.Split(treeNumber, ".")
	node.addChildren(parts, 0, descriptorUi, descriptorName)

}

func (node *Node) addChildren(nodes []string, d int, descriptorUi string, descriptorName string) {

	if d == len(nodes) {
		return
	}
	//log.Println(nodes, d, descriptorUi, descriptorName)
	//log.Println(nodes[d])
	thisNodeLabel := nodes[d]
	for _, n := range node.ChildNodes {
		if n != nil && n.NodeLabel == thisNodeLabel {
			// already exists
			n.addChildren(nodes, d+1, descriptorUi, descriptorName)
			if d+1 == len(nodes) {
				n.Name = descriptorName
			}
			return
		}
	}
	// node does not exist
	n := InitializeNode()
	n.NodeLabel = thisNodeLabel
	n.TreeNumber = strings.Join(nodes[0:d], ".")
	n.RecordId = descriptorUi
	if d+1 == len(nodes) {
		n.Name = descriptorName
	}
	node.ChildNodes = append(node.ChildNodes, n)
	node.ChildIds = append(node.ChildIds, descriptorUi)

}

type visitor func(*Node)

func (node *Node) DepthTraverse(depth int, v visitor) {
	if v != nil {
		v(node)
	}
	log.Println(spaces(depth), depth, node.TreeNumber, node.NodeLabel, node.RecordId, node.Name, node.ChildIds)
	for _, child := range node.ChildNodes {
		child.DepthTraverse(depth+1, v)
	}
}

func spaces(n int) string {
	s := ""
	for i := 0; i <= n; i++ {
		s = s + " "
	}
	return s
}
