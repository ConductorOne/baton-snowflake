package connector

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// memorySessionStore is a minimal in-memory sessions.SessionStore for builder tests.
type memorySessionStore struct {
	data map[string][]byte
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{data: map[string][]byte{}}
}

func (m *memorySessionStore) key(ctx context.Context, key string, opt []sessions.SessionStoreOption) (string, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		if err := o(ctx, bag); err != nil {
			return "", err
		}
	}
	return bag.Prefix + "/" + key, nil
}

func (m *memorySessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	k, err := m.key(ctx, key, opt)
	if err != nil {
		return nil, false, err
	}
	v, ok := m.data[k]
	return v, ok, nil
}

func (m *memorySessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	found := map[string][]byte{}
	var missing []string
	for _, key := range keys {
		v, ok, err := m.Get(ctx, key, opt...)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			found[key] = v
		} else {
			missing = append(missing, key)
		}
	}
	return found, missing, nil
}

func (m *memorySessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	k, err := m.key(ctx, key, opt)
	if err != nil {
		return err
	}
	m.data[k] = value
	return nil
}

func (m *memorySessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	for key, value := range values {
		if err := m.Set(ctx, key, value, opt...); err != nil {
			return err
		}
	}
	return nil
}

func (m *memorySessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	k, err := m.key(ctx, key, opt)
	if err != nil {
		return err
	}
	delete(m.data, k)
	return nil
}

func (m *memorySessionStore) Clear(context.Context, ...sessions.SessionStoreOption) error {
	m.data = map[string][]byte{}
	return nil
}

func (m *memorySessionStore) GetAll(context.Context, string, ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return m.data, "", nil
}

var _ sessions.SessionStore = (*memorySessionStore)(nil)
