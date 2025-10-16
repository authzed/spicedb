package query

// Walk traverses an iterator tree depth-first, calling the callback for each node.
// If the callback returns a different iterator than the input, that iterator replaces the current node.
// The callback is applied bottom-up (children are processed before parents).
func Walk(root Iterator, callback func(Iterator) Iterator) Iterator {
	if root == nil {
		return nil
	}

	// First, recursively walk all subiterators
	subIts := root.Subiterators()
	if len(subIts) > 0 {
		// Process children first (bottom-up traversal)
		processedSubs := make([]Iterator, len(subIts))
		for i, subIt := range subIts {
			processedSubs[i] = Walk(subIt, callback)
		}

		// Replace subiterators in the parent if they changed
		root = root.ReplaceSubiterators(processedSubs)
	}

	// Apply callback to this node (after children have been processed)
	return callback(root)
}
