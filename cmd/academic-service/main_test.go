package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDegradedResponseIsTheRealCachedRecord(t *testing.T) {
	c := &transcriptCache{records: map[int]cachedTranscript{}}
	if _, ok := c.degraded(1, "r1"); ok {
		t.Fatal("empty cache must not produce a transcript")
	}

	c.put(TranscriptRecord{StudentID: 1, GPA: 3.33, Credits: 7, Details: json.RawMessage(`{}`), Status: "OFFICIAL"})
	got, ok := c.degraded(1, "r2")
	if !ok || got.GPA != 3.33 || got.Credits != 7 {
		t.Fatalf("degraded record differs from stored one: %+v", got)
	}
	if got.Status != "DEGRADED_CACHED_MODE" || got.ServedBy != "r2" || !strings.Contains(got.DegradedNotice, "copy from") {
		t.Fatalf("degraded record is not labelled: %+v", got)
	}
	if _, ok := c.degraded(2, "r2"); ok {
		t.Fatal("cache must not answer for another student")
	}
}

func TestTranscriptStudentID(t *testing.T) {
	cases := []struct {
		path, user string
		want       int
		wantErr    bool
	}{
		{"/api/transcripts/7", "1", 7, false},
		{"/api/transcripts", "3", 3, false},
		{"/api/transcripts/abc", "1", 0, true},
		{"/api/transcripts/1;DROP", "1", 0, true},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "http://x"+strings.ReplaceAll(c.path, ";", "%3B"), nil)
		r.Header.Set("X-User-ID", c.user)
		got, err := transcriptStudentID(r)
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("%s: got %d err=%v", c.path, got, err)
		}
	}
}
