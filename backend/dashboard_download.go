package main

import (
	"encoding/base64"
	"fmt"
	"time"
)

type dashboardDownload struct {
	Content  []byte
	Offset   int
	Created  time.Time
	Expected int
	Staged   bool
}

func (s *session) dashboardDownload(method string, p map[string]any) (any, error) {
	s.operations.RLock()
	defer s.operations.RUnlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, fmt.Errorf("connection is closed")
	}
	id := stringValue(p, "downloadId")
	if id == "" {
		return nil, fmt.Errorf("downloadId is required")
	}
	d := s.dashboard
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.downloads == nil {
		d.downloads = map[string]*dashboardDownload{}
	}
	for key, value := range d.downloads {
		if time.Since(value.Created) > 10*time.Minute {
			delete(d.downloads, key)
		}
	}
	switch method {
	case "filesystem/download/stage":
		expected := intValue(p, -1, "size")
		if expected < 0 || expected > 8*1024*1024 {
			return nil, fmt.Errorf("export exceeds 8 MiB or invalid size")
		}
		if len(d.downloads) >= 2 {
			return nil, fmt.Errorf("too many downloads")
		}
		d.downloads[id] = &dashboardDownload{Content: make([]byte, 0, expected), Created: time.Now(), Expected: expected, Staged: true}
		return true, nil
	case "filesystem/download/append":
		item := d.downloads[id]
		if item == nil || !item.Staged {
			return nil, fmt.Errorf("download staging expired")
		}
		if intValue(p, -1, "offset") != len(item.Content) {
			return nil, fmt.Errorf("invalid export chunk offset")
		}
		chunk, err := base64.StdEncoding.DecodeString(stringValue(p, "dataBase64"))
		if err != nil || len(chunk) > 256*1024 || len(item.Content)+len(chunk) > item.Expected {
			return nil, fmt.Errorf("invalid export chunk")
		}
		item.Content = append(item.Content, chunk...)
		return true, nil
	case "filesystem/download/open":
		if item := d.downloads[id]; item != nil && item.Staged {
			if len(item.Content) != item.Expected {
				return nil, fmt.Errorf("export upload incomplete")
			}
			item.Staged = false
			return map[string]any{"size": len(item.Content)}, nil
		}
		text, _ := p["content"].(string)
		content := []byte(text)
		if encoded, ok := p["contentBase64"].(string); ok {
			var err error
			content, err = base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return nil, fmt.Errorf("invalid download base64")
			}
		}
		if len(content) > 8*1024*1024 {
			return nil, fmt.Errorf("export exceeds 8 MiB")
		}
		if len(d.downloads) >= 2 {
			return nil, fmt.Errorf("too many downloads")
		}
		d.downloads[id] = &dashboardDownload{Content: content, Created: time.Now()}
		return map[string]any{"size": len(content)}, nil
	case "filesystem/download/read":
		item := d.downloads[id]
		if item == nil || item.Staged {
			return nil, fmt.Errorf("download expired")
		}
		end := minValue(item.Offset+256*1024, len(item.Content))
		chunk := item.Content[item.Offset:end]
		item.Offset = end
		return map[string]any{"dataBase64": base64.StdEncoding.EncodeToString(chunk), "done": end == len(item.Content)}, nil
	case "filesystem/download/close":
		delete(d.downloads, id)
		return true, nil
	}
	return nil, fmt.Errorf("unknown download method")
}
