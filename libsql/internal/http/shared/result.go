package shared

type result struct {
	id      int64
	changes int64
}

func NewResult(id, changes int64) *result {
	return &result{id: id, changes: changes}
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.changes, nil
}
