package libsql

import (
	"database/sql"
	"net/url"
	"testing"
)

// Test helper functions

func TestContains(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		value    string
		expected bool
	}{
		{"found", []string{"a", "b", "c"}, "b", true},
		{"not found", []string{"a", "b", "c"}, "d", false},
		{"empty slice", []string{}, "a", false},
		{"single element found", []string{"a"}, "a", true},
		{"single element not found", []string{"a"}, "b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Contains(tt.slice, tt.value)
			if result != tt.expected {
				t.Errorf("Contains(%v, %q) = %v, want %v", tt.slice, tt.value, result, tt.expected)
			}
		})
	}
}

func TestIndex(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		value    string
		expected int
	}{
		{"found at start", []string{"a", "b", "c"}, "a", 0},
		{"found in middle", []string{"a", "b", "c"}, "b", 1},
		{"found at end", []string{"a", "b", "c"}, "c", 2},
		{"not found", []string{"a", "b", "c"}, "d", -1},
		{"empty slice", []string{}, "a", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Index(tt.slice, tt.value)
			if result != tt.expected {
				t.Errorf("Index(%v, %q) = %v, want %v", tt.slice, tt.value, result, tt.expected)
			}
		})
	}
}

// Test extractJwt function

func TestExtractJwt(t *testing.T) {
	tests := []struct {
		name        string
		queryParams map[string]string
		expectedJwt string
		expectError bool
	}{
		{
			name:        "auth_token",
			queryParams: map[string]string{"auth_token": "token123"},
			expectedJwt: "token123",
			expectError: false,
		},
		{
			name:        "authToken",
			queryParams: map[string]string{"authToken": "token456"},
			expectedJwt: "token456",
			expectError: false,
		},
		{
			name:        "jwt",
			queryParams: map[string]string{"jwt": "token789"},
			expectedJwt: "token789",
			expectError: false,
		},
		{
			name:        "no token",
			queryParams: map[string]string{},
			expectedJwt: "",
			expectError: false,
		},
		{
			name:        "multiple tokens",
			queryParams: map[string]string{"auth_token": "token1", "jwt": "token2"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := url.Values{}
			for k, v := range tt.queryParams {
				query.Set(k, v)
			}

			jwt, err := extractJwt(&query)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if jwt != tt.expectedJwt {
					t.Errorf("extractJwt() = %q, want %q", jwt, tt.expectedJwt)
				}
				// Verify parameters are deleted
				if query.Get("auth_token") != "" || query.Get("authToken") != "" || query.Get("jwt") != "" {
					t.Errorf("query parameters should be deleted after extraction")
				}
			}
		})
	}
}

// Test extractTls function

func TestExtractTls(t *testing.T) {
	tests := []struct {
		name        string
		queryValue  string
		scheme      string
		expectedTls bool
		expectError bool
	}{
		{"no tls param http", "", "http", false, false},
		{"no tls param https", "", "https", true, false},
		{"no tls param ws", "", "ws", false, false},
		{"no tls param wss", "", "wss", true, false},
		{"tls=0", "0", "https", false, false},
		{"tls=1", "1", "http", true, false},
		{"invalid tls value", "invalid", "https", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := url.Values{}
			if tt.queryValue != "" {
				query.Set("tls", tt.queryValue)
			}

			tls, err := extractTls(&query, tt.scheme)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tls != tt.expectedTls {
					t.Errorf("extractTls(%q, %q) = %v, want %v", tt.queryValue, tt.scheme, tls, tt.expectedTls)
				}
			}
		})
	}
}

// Test Option functions

func TestWithAuthToken(t *testing.T) {
	tests := []struct {
		name        string
		token       string
		expectError bool
	}{
		{"valid token", "mytoken123", false},
		{"empty token", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg config
			opt := WithAuthToken(tt.token)
			err := opt.apply(&cfg)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.authToken == nil || *cfg.authToken != tt.token {
					t.Errorf("authToken not set correctly")
				}
			}
		})
	}

	t.Run("duplicate auth token", func(t *testing.T) {
		var cfg config
		opt1 := WithAuthToken("token1")
		opt2 := WithAuthToken("token2")

		_ = opt1.apply(&cfg)
		err := opt2.apply(&cfg)

		if err == nil {
			t.Errorf("expected error when setting authToken twice")
		}
	})
}

func TestWithTls(t *testing.T) {
	tests := []struct {
		name  string
		value bool
	}{
		{"tls true", true},
		{"tls false", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg config
			opt := WithTls(tt.value)
			err := opt.apply(&cfg)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if cfg.tls == nil || *cfg.tls != tt.value {
				t.Errorf("tls not set correctly")
			}
		})
	}

	t.Run("duplicate tls", func(t *testing.T) {
		var cfg config
		opt1 := WithTls(true)
		opt2 := WithTls(false)

		_ = opt1.apply(&cfg)
		err := opt2.apply(&cfg)

		if err == nil {
			t.Errorf("expected error when setting tls twice")
		}
	})
}

func TestWithProxy(t *testing.T) {
	tests := []struct {
		name        string
		proxy       string
		expectError bool
	}{
		{"valid proxy", "http://proxy.example.com:8080", false},
		{"empty proxy", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg config
			opt := WithProxy(tt.proxy)
			err := opt.apply(&cfg)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if cfg.proxy == nil || *cfg.proxy != tt.proxy {
					t.Errorf("proxy not set correctly")
				}
			}
		})
	}

	t.Run("duplicate proxy", func(t *testing.T) {
		var cfg config
		opt1 := WithProxy("http://proxy1.com")
		opt2 := WithProxy("http://proxy2.com")

		_ = opt1.apply(&cfg)
		err := opt2.apply(&cfg)

		if err == nil {
			t.Errorf("expected error when setting proxy twice")
		}
	})
}

func TestWithSchemaDb(t *testing.T) {
	tests := []struct {
		name  string
		value bool
	}{
		{"schemaDb true", true},
		{"schemaDb false", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg config
			opt := WithSchemaDb(tt.value)
			err := opt.apply(&cfg)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if cfg.schemaDb == nil || *cfg.schemaDb != tt.value {
				t.Errorf("schemaDb not set correctly")
			}
		})
	}

	t.Run("duplicate schemaDb", func(t *testing.T) {
		var cfg config
		opt1 := WithSchemaDb(true)
		opt2 := WithSchemaDb(false)

		_ = opt1.apply(&cfg)
		err := opt2.apply(&cfg)

		if err == nil {
			t.Errorf("expected error when setting schemaDb twice")
		}
	})
}

// Test NewConnector

func TestNewConnector_InvalidURLs(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "invalid scheme",
			url:         "ftp://example.com",
			expectError: true,
			errorMsg:    "unsupported URL scheme",
		},
		{
			name:        "empty scheme",
			url:         "example.com",
			expectError: true,
			errorMsg:    "unsupported URL scheme",
		},
		{
			name:        "invalid file URL",
			url:         "file://path/to/db",
			expectError: true,
			errorMsg:    "File URLs should not have double leading slashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.url)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewConnector_QueryParamForbidden(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		forbidden string
	}{
		{"auth_token in query", "https://example.com?auth_token=token", "auth_token"},
		{"authToken in query", "https://example.com?authToken=token", "authToken"},
		{"jwt in query", "https://example.com?jwt=token", "jwt"},
		{"tls in query", "https://example.com?tls=1", "tls"},
		{"unknown param", "https://example.com?foo=bar", "unknown query parameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.url)

			if err == nil {
				t.Errorf("expected error for forbidden query param but got none")
			}
		})
	}
}

func TestNewConnector_LibsqlScheme(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		opts        []Option
		expectError bool
	}{
		{
			name: "libsql with default tls",
			url:  "libsql://example.com",
			opts: nil,
		},
		{
			name: "libsql with tls=true",
			url:  "libsql://example.com",
			opts: []Option{WithTls(true)},
		},
		{
			name: "libsql with tls=false and port",
			url:  "libsql://example.com:8080",
			opts: []Option{WithTls(false)},
		},
		{
			name:        "libsql with tls=false without port",
			url:         "libsql://example.com",
			opts:        []Option{WithTls(false)},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.url, tt.opts...)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewConnector_TLSValidation(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		opts        []Option
		expectError bool
		errorMsg    string
	}{
		{
			name:        "https cannot opt out of TLS",
			url:         "https://example.com",
			opts:        []Option{WithTls(false)},
			expectError: true,
			errorMsg:    "cannot opt out of TLS",
		},
		{
			name:        "wss cannot opt out of TLS",
			url:         "wss://example.com",
			opts:        []Option{WithTls(false)},
			expectError: true,
			errorMsg:    "cannot opt out of TLS",
		},
		{
			name:        "http cannot opt in to TLS",
			url:         "http://example.com",
			opts:        []Option{WithTls(true)},
			expectError: true,
			errorMsg:    "cannot opt in to TLS",
		},
		{
			name:        "ws cannot opt in to TLS",
			url:         "ws://example.com",
			opts:        []Option{WithTls(true)},
			expectError: true,
			errorMsg:    "cannot opt in to TLS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.url, tt.opts...)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewConnector_WebSocketSchemes(t *testing.T) {
	tests := []struct {
		name string
		url  string
		opts []Option
	}{
		{"wss scheme", "wss://example.com", nil},
		{"ws scheme", "ws://example.com", nil},
		{"wss with auth", "wss://example.com", []Option{WithAuthToken("token123")}},
		{"ws with auth", "ws://example.com", []Option{WithAuthToken("token123")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := NewConnector(tt.url, tt.opts...)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if connector == nil {
				t.Errorf("expected connector but got nil")
			}

			// Verify it's a wsConnector
			if _, ok := connector.(wsConnector); !ok {
				t.Errorf("expected wsConnector but got %T", connector)
			}
		})
	}
}

func TestNewConnector_HTTPSchemes(t *testing.T) {
	tests := []struct {
		name string
		url  string
		opts []Option
	}{
		{"https scheme", "https://example.com", nil},
		{"http scheme", "http://example.com", nil},
		{"https with auth", "https://example.com", []Option{WithAuthToken("token123")}},
		{"http with schemaDb", "http://example.com", []Option{WithSchemaDb(true)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := NewConnector(tt.url, tt.opts...)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if connector == nil {
				t.Errorf("expected connector but got nil")
			}

			// Verify it's an httpConnector
			if _, ok := connector.(httpConnector); !ok {
				t.Errorf("expected httpConnector but got %T", connector)
			}
		})
	}
}

func TestNewConnector_Proxy(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		opts        []Option
		expectError bool
	}{
		{
			name: "http with proxy",
			url:  "http://example.com",
			opts: []Option{WithProxy("http://proxy.com:8080")},
		},
		{
			name: "https with proxy",
			url:  "https://example.com",
			opts: []Option{WithProxy("http://proxy.com:8080")},
		},
		{
			name:        "ws with proxy",
			url:         "ws://example.com",
			opts:        []Option{WithProxy("http://proxy.com:8080")},
			expectError: true,
		},
		{
			name:        "wss with proxy",
			url:         "wss://example.com",
			opts:        []Option{WithProxy("http://proxy.com:8080")},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConnector(tt.url, tt.opts...)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewConnector_MultipleOptions(t *testing.T) {
	connector, err := NewConnector(
		"https://example.com",
		WithAuthToken("mytoken"),
		WithSchemaDb(true),
	)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	httpConn, ok := connector.(httpConnector)
	if !ok {
		t.Fatalf("expected httpConnector but got %T", connector)
	}

	if httpConn.authToken != "mytoken" {
		t.Errorf("authToken = %q, want %q", httpConn.authToken, "mytoken")
	}
	if !httpConn.schemaDb {
		t.Errorf("schemaDb = false, want true")
	}
}

// Test Driver.Open method

func TestDriver_Open_InvalidURLs(t *testing.T) {
	driver := Driver{}

	tests := []struct {
		name string
		url  string
	}{
		{"invalid scheme", "ftp://example.com"},
		{"empty scheme", "example.com"},
		{"invalid file URL", "file://path/to/db"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := driver.Open(tt.url)
			if err == nil {
				t.Errorf("expected error but got none")
			}
		})
	}
}

func TestDriver_Open_LibsqlScheme(t *testing.T) {
	driver := Driver{}

	tests := []struct {
		name        string
		url         string
		expectError bool
	}{
		{"libsql default tls", "libsql://example.com", false},
		{"libsql with tls=1", "libsql://example.com?tls=1", false},
		{"libsql with tls=0 and port", "libsql://example.com:8080?tls=0", false},
		{"libsql with tls=0 without port", "libsql://example.com?tls=0", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := driver.Open(tt.url)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestDriver_Open_TLSValidation(t *testing.T) {
	driver := Driver{}

	tests := []struct {
		name string
		url  string
	}{
		{"https cannot opt out", "https://example.com?tls=0"},
		{"wss cannot opt out", "wss://example.com?tls=0"},
		{"http cannot opt in", "http://example.com?tls=1"},
		{"ws cannot opt in", "ws://example.com?tls=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := driver.Open(tt.url)
			if err == nil {
				t.Errorf("expected error but got none")
			}
		})
	}
}

func TestDriver_RegisteredWithSQL(t *testing.T) {
	drivers := sql.Drivers()
	found := false
	for _, d := range drivers {
		if d == "libsql" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("libsql driver not registered with database/sql")
	}
}
