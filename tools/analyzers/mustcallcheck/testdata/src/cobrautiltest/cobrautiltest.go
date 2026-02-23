package cobrautiltest

// Mock Command type to avoid external dependencies
type Command struct{}

// Mock cobrautil package functions for testing
var cobrautil = struct {
	MustGetString         func(*Command, string) string
	MustGetInt            func(*Command, string) int
	MustGetBool           func(*Command, string) bool
	MustGetStringExpanded func(*Command, string) string
	MustGetDuration       func(*Command, string) int64
}{
	MustGetString: func(cmd *Command, flag string) string {
		return ""
	},
	MustGetInt: func(cmd *Command, flag string) int {
		return 0
	},
	MustGetBool: func(cmd *Command, flag string) bool {
		return false
	},
	MustGetStringExpanded: func(cmd *Command, flag string) string {
		return ""
	},
	MustGetDuration: func(cmd *Command, flag string) int64 {
		return 0
	},
}

// Example 1: cobrautil.MustGetString in error-returning function (OK - exception)
func GetConfigValue(cmd *Command) (string, error) {
	value := cobrautil.MustGetString(cmd, "config")
	return value, nil
}

// Example 2: cobrautil.MustGetInt in error-returning function (OK - exception)
func GetTimeout(cmd *Command) (int, error) {
	timeout := cobrautil.MustGetInt(cmd, "timeout")
	return timeout, nil
}

// Example 3: cobrautil.MustGetBool in error-returning function (OK - exception)
func IsEnabled(cmd *Command) (bool, error) {
	enabled := cobrautil.MustGetBool(cmd, "enabled")
	return enabled, nil
}

// Example 4: Multiple cobrautil Must* calls in error-returning function (OK - exception)
func LoadConfiguration(cmd *Command) (Config, error) {
	return Config{
		Host:    cobrautil.MustGetStringExpanded(cmd, "host"),
		Port:    cobrautil.MustGetInt(cmd, "port"),
		Enabled: cobrautil.MustGetBool(cmd, "enabled"),
		Timeout: cobrautil.MustGetDuration(cmd, "timeout"),
	}, nil
}

// Helper types
type Config struct {
	Host    string
	Port    int
	Enabled bool
	Timeout int64
}
