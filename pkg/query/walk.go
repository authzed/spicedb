package query

// Walk traverses an iterator tree depth-first, calling the callback for each node.
// If the callback returns a different iterator than the input, that iterator replaces the current node.
// The callback is applied bottom-up (children are processed before parents).
// Panics if ReplaceSubiterators returns an error (should never happen in normal operation).
func Walk(root Iterator, callback func(Iterator) (Iterator, error)) (Iterator, error) {
	if root == nil {
		return nil, nil
	}

	var err error
	// First, recursively walk all subiterators
	subIts := root.Subiterators()
	if len(subIts) > 0 {
		// Process children first (bottom-up traversal)
		processedSubs := make([]Iterator, len(subIts))
		for i, subIt := range subIts {
			processedSubs[i], err = Walk(subIt, callback)
			if err != nil {
				return nil, err
			}
		}

		// Replace subiterators in the parent if they changed
		var err error
		root, err = root.ReplaceSubiterators(processedSubs)
		if err != nil {
			return nil, err
		}
	}

	// Apply callback to this node (after children have been processed)
	return callback(root)
}
