package holder

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	controller_service "github.com/kregonia/brander_mixer/script/rpc_server/controller"
	"google.golang.org/protobuf/proto"
)

const (
	filePrefix            = "status_log_"
	timeStampDataFolder   = "./data/timestamp/"
	defaultTimeDifference = 60 // 默认刷新时间，单位秒
	defaultRefreshTimes   = 10 // 默认刷新次数
)

var (
	flashChannel = make(chan func(), 1)
)

type StatusSlice struct {
	file                *os.File
	BeginTimestamp      int64
	TimeDifference      int64
	Status              controller_service.RepeatedStatus
	refreshTimes        uint
	defaultRefreshTimes uint
	sync.RWMutex
}

type StatusHolder struct {
	m sync.Map
}

func NewStatusSlice(beginTimeStamp int64, refreshTimes uint, file *os.File) *StatusSlice {
	return &StatusSlice{
		file:                file,
		BeginTimestamp:      beginTimeStamp,
		TimeDifference:      defaultTimeDifference * int64(time.Second),
		Status:              controller_service.RepeatedStatus{Statuses: make([]*controller_service.Status, 0)},
		refreshTimes:        refreshTimes,
		defaultRefreshTimes: refreshTimes,
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

func (ss *StatusSlice) GetLength() int {
	ss.RLock()
	defer ss.RUnlock()
	return len(ss.Status.Statuses)
}

func (ss *StatusSlice) AppendStatus(status *controller_service.Status) {
	ss.Lock()
	defer ss.Unlock()
	ss.Status.Statuses = append(ss.Status.Statuses, status)
}

func NewStatusHolder(refreshTimes uint) *StatusHolder {
	return &StatusHolder{
		m: sync.Map{},
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

func (sh *StatusHolder) AppendStatusByKey(key string, value *controller_service.Status) {
	defer sh.DecreaseRefreshTimeByKey(key)
	v, ok := sh.m.Load(key)
	if !ok {
		// 第一次存储该key
		fileName := fmt.Sprintf("%s%s%s_%s.brander", timeStampDataFolder, filePrefix, key, time.Now().Format("20060102"))
		if _, err := os.Stat(timeStampDataFolder); os.IsNotExist(err) {
			if err := os.MkdirAll(timeStampDataFolder, 0755); err != nil {
				logger.Errorf("[controller Flash2Disk] error,can't create folder %s ,err:%v\n", timeStampDataFolder, err)
				return
			}
		}
		f, err := os.OpenFile(
			fileName,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644,
		)
		if err != nil {
			logger.Errorf("[controller Flash2Disk] error,can't open or create file %s ,err:%v\n", fileName, err)
			return
		}
		// end
		ss := NewStatusSlice(time.Now().Unix(), defaultRefreshTimes, f)
		ss.AppendStatus(value)
		sh.m.Store(key, ss)
		return
	}
	v.(*StatusSlice).AppendStatus(value)

}

func (sh *StatusHolder) Delete(key string) {
	sh.m.Delete(key)
}

func (sh *StatusHolder) CopyByKey(key string) *StatusSlice {
	value, ok := sh.m.Load(key)
	if !ok {
		return nil
	}
	copyValue := NewStatusSlice(value.(*StatusSlice).BeginTimestamp, defaultRefreshTimes, value.(*StatusSlice).file)
	copyValue.Lock()
	for _, status := range value.(*StatusSlice).Status.Statuses {
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
	return copyValue
}

func (sh *StatusHolder) GetMap() *sync.Map {
	return &sh.m
}

func (sh *StatusHolder) DecreaseRefreshTimeByKey(key string) {
	v, ok := sh.m.Load(key)
	if !ok {
		return
	}
	v.(*StatusSlice).Lock()
	defer v.(*StatusSlice).Unlock()
	if v.(*StatusSlice).refreshTimes > 1 {
		v.(*StatusSlice).refreshTimes--
	} else {
		v.(*StatusSlice).refreshTimes = defaultRefreshTimes
		flashChannel <- func() {
			flashChannel <- func() {}
			sh.Flash2DiskByKey(key)
			<-flashChannel
		}
		do := <-flashChannel
		go do()
	}
}

func (sh *StatusHolder) Flash2DiskByKey(key string) {
	// todo: 对每个worker的status进行落盘
	value := sh.CopyByKey(key)
	sh.Delete(key)
	sh.Store(key, NewStatusSlice(time.Now().Unix(), defaultRefreshTimes, value.file))
	// key 是worker的ip地址
	fileName := fmt.Sprintf("%s%s%s_%s.brander", timeStampDataFolder, filePrefix, key, time.Now().Format("20060102"))
	if _, err := os.Stat(timeStampDataFolder); os.IsNotExist(err) {
		if err := os.MkdirAll(timeStampDataFolder, 0755); err != nil {
			logger.Errorf("[controller Flash2Disk] error,can't create folder %s ,err:%v\n", timeStampDataFolder, err)
			return
		}
	}
	value.RLock()
	defer value.RUnlock()
	data, err := value.ToBytes()
	if err != nil {
		logger.Errorf("[controller Flash2Disk] error,can't marshal statusSlice ,err:%v\n", err)
		return
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	// 先写长度
	if _, err := value.file.Write(lenBuf[:]); err != nil {
		logger.Errorf("[controller Flash2Disk] error,can't write length to file %s ,err:%v\n", fileName, err)
		return
	}
	_, err = value.file.Write(data)
	if err != nil {
		logger.Errorf("[controller Flash2Disk] error,can't write to file %s ,err:%v\n", fileName, err)
	}
}
