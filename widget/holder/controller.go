package holder

import (
	"sync"

	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
)

const (
	filePrefix = "status_log_"
)

type StatusSlice struct {
	BeginTimestamp int64
	TimeDifference int64
	Status         []*controller_service.Status
	sync.RWMutex
}

type StatusHolder struct {
	m                  sync.Map
	refreshTime        uint
	defaultRefreshTime uint
	sync.RWMutex
}

func NewStatusHolder(refreshTime uint) *StatusHolder {
	return &StatusHolder{
		m:                  sync.Map{},
		refreshTime:        refreshTime,
		defaultRefreshTime: refreshTime,
	}
}

func (sh *StatusHolder) GetMap() *sync.Map {
	return &sh.m
}

func (sh *StatusHolder) GetDefaultRefreshTime() uint {
	return sh.defaultRefreshTime
}

func (sh *StatusHolder) DecreaseRefreshTime() {
	sh.Lock()
	defer sh.Unlock()
	if sh.refreshTime >= 1 {
		sh.refreshTime--
	}
	if sh.refreshTime == 0 {
		// todo:write the status log to disk
		// end
		sh.reSetRefreshTime()
	}
}

func (sh *StatusHolder) reSetRefreshTime() {
	sh.refreshTime = sh.defaultRefreshTime
}

func (sh *StatusHolder) Flash2Disk() {
	// todo: 对每个worker的status进行落盘
}
