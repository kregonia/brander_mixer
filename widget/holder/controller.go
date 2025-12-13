package holder

import (
	"fmt"
	"os"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/protobuf/proto"
)

const (
	filePrefix = "status_log_"
)

const (
	DefaultTimeDifference = 60 // 默认刷新时间，单位秒
)

type StatusSlice struct {
	BeginTimestamp int64
	TimeDifference int64
	Status         controller_service.RepeatedStatus
	sync.RWMutex
}

type StatusHolder struct {
	m                   sync.Map
	refreshTimes        uint
	defaultRefreshTimes uint
	sync.RWMutex
}

func NewStatusSlice(beginTimeStamp int64) *StatusSlice {
	return &StatusSlice{
		BeginTimestamp: beginTimeStamp,
		TimeDifference: DefaultTimeDifference * int64(time.Second),
		Status:         controller_service.RepeatedStatus{Statuses: make([]*controller_service.Status, 0)},
	}
}

func (ss *StatusSlice) ToBytes() ([]byte, error) {
	ss.RLock()
	defer ss.RUnlock()
	result, err := proto.Marshal(&ss.Status)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func NewStatusHolder(refreshTimes uint) *StatusHolder {
	return &StatusHolder{
		m:                   sync.Map{},
		refreshTimes:        refreshTimes,
		defaultRefreshTimes: refreshTimes,
	}
}
func (sh *StatusHolder) Store(key string, value *StatusSlice) {
	sh.m.Store(key, value)
}

func (sh *StatusHolder) Load(key string) (*StatusSlice, bool) {
	v, ok := sh.m.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*StatusSlice), true
}

func (sh *StatusHolder) Delete(key string) {
	sh.m.Delete(key)
}

func (sh *StatusHolder) Copy() *StatusHolder {
	newHolder := NewStatusHolder(sh.defaultRefreshTimes)
	sh.m.Range(func(key, value any) bool {
		copyValue := NewStatusSlice(value.(*StatusSlice).BeginTimestamp)
		copyValue.Lock()
		for _, status := range value.(*controller_service.RepeatedStatus).Statuses {
			copyStatus := &controller_service.Status{
				CpuUsage:     status.CpuUsage,
				CpuCores:     status.CpuCores,
				CpuFrequency: status.CpuFrequency,
				MemoryUsage:  status.MemoryUsage,
				MemoryTotal:  status.MemoryTotal,
				TaskCount:    status.TaskCount,
			}
			copyValue.Status.Statuses = append(copyValue.Status.Statuses, copyStatus)
		}
		copyValue.Unlock()
		newHolder.m.Store(key, copyValue)
		return true
	})
	return newHolder
}

func (sh *StatusHolder) GetMap() *sync.Map {
	return &sh.m
}

func (sh *StatusHolder) GetDefaultRefreshTime() uint {
	return sh.defaultRefreshTimes
}

func (sh *StatusHolder) DecreaseRefreshTime() {
	sh.Lock()
	defer sh.Unlock()
	if sh.refreshTimes >= 1 {
		sh.refreshTimes--
	}
	if sh.refreshTimes == 0 {
		// todo:write the status log to disk
		sh.Flash2Disk()
		// end
		sh.reSetRefreshTime()
	}
}

func (sh *StatusHolder) reSetRefreshTime() {
	sh.refreshTimes = sh.defaultRefreshTimes
}

func (sh *StatusHolder) Flash2Disk() {
	// todo: 对每个worker的status进行落盘
	value := sh.Copy()
	value.m.Range(func(key, val any) bool {
		// key 是worker的ip地址
		fileName := fmt.Sprintf("%s%s_%s.brander", filePrefix, key.(string), time.Now().Format("20060102"))
		f, err := os.OpenFile(
			fileName,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644,
		)
		if err != nil {
			logger.Errorf("[controller Flash2Disk] error,can't open or create file %s ,err:%v\n", fileName, err)
			return true
		}
		defer f.Close()
		statusSlice := val.(*StatusSlice)
		statusSlice.RLock()
		defer statusSlice.RUnlock()
		data, err := statusSlice.ToBytes()
		if err != nil {
			logger.Errorf("[controller Flash2Disk] error,can't marshal statusSlice ,err:%v\n", err)
			return true
		}
		_, err = f.Write(data)
		if err != nil {
			logger.Errorf("[controller Flash2Disk] error,can't write to file %s ,err:%v\n", fileName, err)
		}
		return true
	})
}
