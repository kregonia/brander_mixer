package holder

import (
	"sync"

	"github.com/kregonia/brander_mixer/widget/tool"
)

type WorkerAliveSecrets struct {
	sync.Map
}

func NewWorkerAliveSecrets() *WorkerAliveSecrets {
	return &WorkerAliveSecrets{}
}

func (was *WorkerAliveSecrets) SetSecret(ip string) {
	secret := tool.RandomString(16)
	was.Store(ip, secret)
}

func (was *WorkerAliveSecrets) CompareSecret(ip, secret string) bool {
	secretBytes, ok := was.Load(ip)
	if !ok {
		return false
	}
	return secretBytes.(string) == secret
}

func (was *WorkerAliveSecrets) DeleteSecret(ip string) {
	was.Delete(ip)
}

func (was *WorkerAliveSecrets) GetSecret(ip string) (string, bool) {
	secretBytes, ok := was.Load(ip)
	if !ok {
		return "", false
	}
	return secretBytes.(string), true
}
