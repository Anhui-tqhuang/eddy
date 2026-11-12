package eddy

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
)

// column is one struct field mapped to a table column.
type column struct {
	name       string
	index      []int // reflect field path
	status     bool  // written only by the controller
	hasDefault bool  // omitted on insert when the field is zero
}

// columnsOf maps the exported fields of struct type t to columns. The column
// name comes from the `db` tag, or the snake_case field name without one.
// Tag options: "default" and "status"; `db:"-"` skips the field. Embedded
// structs are flattened.
func columnsOf(t reflect.Type) ([]column, error) {
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%s is not a struct", t)
	}
	var cols []column
	seen := map[string]bool{}
	var walk func(t reflect.Type, path []int) error
	walk = func(t reflect.Type, path []int) error {
		for i := range t.NumField() {
			f := t.Field(i)
			tag, tagged := f.Tag.Lookup("db")
			idx := append(append([]int{}, path...), i)
			if f.Anonymous && !tagged && f.Type.Kind() == reflect.Struct {
				if err := walk(f.Type, idx); err != nil {
					return err
				}
				continue
			}
			if !f.IsExported() || tag == "-" {
				continue
			}
			name, opts, _ := strings.Cut(tag, ",")
			if name == "" {
				name = snakeCase(f.Name)
			}
			c := column{name: name, index: idx}
			for _, opt := range strings.Split(opts, ",") {
				switch opt {
				case "":
				case "status":
					c.status = true
				case "default":
					c.hasDefault = true
				default:
					return fmt.Errorf("field %s: unknown db tag option %q", f.Name, opt)
				}
			}
			if seen[name] {
				return fmt.Errorf("column %q mapped twice", name)
			}
			seen[name] = true
			cols = append(cols, c)
		}
		return nil
	}
	if err := walk(t, nil); err != nil {
		return nil, err
	}
	return cols, nil
}

// snakeCase turns a Go field name into a column name: CreatedAt becomes
// created_at, TeamID team_id and HTTPPort http_port.
func snakeCase(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		if unicode.IsUpper(c) && i > 0 {
			prev := r[i-1]
			nextLower := i+1 < len(r) && unicode.IsLower(r[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(c))
	}
	return b.String()
}

// quote sanitises a possibly schema-qualified identifier.
func quote(name string) string {
	return pgx.Identifier(strings.Split(name, ".")).Sanitize()
}

func quoteAll(cols []column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quote(c.name)
	}
	return strings.Join(names, ", ")
}
