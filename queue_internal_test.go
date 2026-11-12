package eddy

import (
	"regexp"
	"strings"
	"testing"
)

// riverQueueName is River's rule for queue names.
var riverQueueName = regexp.MustCompile(`^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$`)

func TestQueueFor_namesAreValidAndStable(t *testing.T) {
	tests := []struct {
		name, kind string
		set        settings
		want       string
	}{
		{"shared_queue_without_limit", "apps", settings{}, "eddy"},
		{"priority_alone_keeps_shared_queue", "apps", settings{priority: 3}, "eddy"},
		{"own_queue", "apps", settings{workers: 2}, "eddy-apps"},
		{"schema_qualified", "Billing.Invoices", settings{workers: 1}, "eddy-billing_invoices"},
		{"separators_collapse", "__a--b..c__", settings{workers: 1}, "eddy-a_b_c"},
		{"nothing_usable", "***", settings{workers: 1}, "eddy-e22c13cd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := queueFor("eddy", tt.kind, tt.set)
			if got != tt.want || !riverQueueName.MatchString(got) {
				t.Fatalf("queueFor(%q) = %q, want %q (and a valid River name)", tt.kind, got, tt.want)
			}
		})
	}
	long := queueFor("eddy", strings.Repeat("a_very_long_kind_name", 5), settings{workers: 1})
	other := queueFor("eddy", strings.Repeat("a_very_long_kind_name", 5)+"x", settings{workers: 1})
	if len(long) > maxQueueName || !riverQueueName.MatchString(long) || long == other {
		t.Fatalf("long kind: %q (len %d), other %q", long, len(long), other)
	}
}
