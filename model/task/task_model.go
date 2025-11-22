package task

import "github.com/kregonia/brander_mixer/model/_const"

type TaskUnit struct {
	Id        string          `json:"id"`
	Type      _const.TaskType `json:"type"`
	Data      []byte          `json:"data"`
	Timestamp int64           `json:"timestap"`
}
