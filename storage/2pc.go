package storage

// committer represents the transaction 2 phase committer. It will calculate the
// mutations and apply to the low-level storage.
type committer struct {
}

// Prepare implements the first stage of 2PC transaction model.
func (c *committer) Prepare() error {
	return nil
}

// Commit implements the second stage of 2PC transaction model.
func (*committer) Commit() error {
	return nil
}
