package rest

import (
	"net/http"
	"testing"
	"time"

	apiv1 "github.com/cozystack/blockstor/pkg/api/v1"
	"github.com/cozystack/blockstor/pkg/store"
)

// An in-progress mark whose writer was killed stood for good, and every
// retry answered 409 until somebody cleared it by hand. A retry now takes
// over a mark older than store.StaleRollbackAfter and resumes; a fresh one
// is still refused.
func TestCloneRetryTakesOverAStaleRollbackMark(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		age  time.Duration
		want int
	}{
		{name: "fresh", age: 10 * time.Second, want: http.StatusConflict},
		{name: "stale", age: store.StaleRollbackAfter + time.Minute, want: http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewInMemory()
			src, dst := "src-stale-"+tc.name, "dst-stale-"+tc.name
			seedDeployedCloneSource(t, st, src)

			props := store.MarkRollback(map[string]string{
				restoreFromSnapshotKey: restoreMarker(src, cloneSnapshotName(dst)),
			}, rollbackInProgress, time.Now().Add(-tc.age))

			if err := st.ResourceDefinitions().Create(t.Context(), &apiv1.ResourceDefinition{Name: dst, Props: props}); err != nil {
				t.Fatalf("seed the leftover: %v", err)
			}

			base, stop := startServerWithStore(t, st)
			defer stop()

			if code := cloneOnce(t, base, src, dst, nil); code != tc.want {
				t.Fatalf("retry over a %s in-progress mark = %d, want %d", tc.name, code, tc.want)
			}
		})
	}
}
