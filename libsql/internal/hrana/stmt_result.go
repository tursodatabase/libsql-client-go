package hrana

import "strconv"

type Column struct {
	Name *string `json:"name"`
	Type *string `json:"decltype"`
}

type StmtResult struct {
	Cols             []Column  `json:"cols"`
	Rows             [][]Value `json:"rows"`
	AffectedRowCount int32     `json:"affected_row_count"`
	LastInsertRowId  *string   `json:"last_insert_rowid"`
}

func (r *StmtResult) GetLastInsertRowId() int64 {
	if r.LastInsertRowId != nil {
		if integer, err := strconv.ParseInt(*r.LastInsertRowId, 10, 64); err == nil {
			return integer
		}
	}
	return 0
}
