package idem

import (
	"testing"
)

func TestRedundHalter(t *testing.T) {
	// Create a halter that will be both parent and grandchild
	root := NewHalter()
	child := NewHalter()
	grandchild := NewHalter()

	// First create a normal parent-child relationship
	root.AddChild(child)
	child.AddChild(grandchild)

	// Now try to make root a grandchild of itself by adding it as a child of grandchild
	// This should panic with a cycle detection message
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic when creating cycle in halter tree")
		} else {
			// Verify the panic message contains the expected text about cycles
			if r != ErrHalterCycle {
				t.Errorf("Expected panic about cycle detection, got: %v", r)
			}
		}
	}()

	// This should trigger the panic
	grandchild.AddChild(root)
}

func TestRedundChan(t *testing.T) {
	// Create a halter that will be both parent and grandchild
	root := NewIdemCloseChan()
	child := NewIdemCloseChan()
	grandchild := NewIdemCloseChan()

	// First create a normal parent-child relationship
	root.AddChild(child)
	child.AddChild(grandchild)

	// Now try to make root a grandchild of itself by adding it as a child of grandchild
	// This should panic with a cycle detection message
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic when creating cycle in IdemCloseChan tree")
		} else {
			// Verify the panic message contains the expected text about cycles
			if r != ErrChanCycle {
				t.Errorf("Expected panic about cycle detection, got: %v", r)
			}
		}
	}()

	// This should trigger the panic
	grandchild.AddChild(root)
}
