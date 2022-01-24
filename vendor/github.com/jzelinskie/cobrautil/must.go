package cobrautil

import (
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// MustGetStringExpanded returns the string value of a flag with the given name,
// calls os.Expand on it, and panics if that flag was never defined.
func MustGetStringExpanded(cmd *cobra.Command, name string) string {
	return os.ExpandEnv(MustGetString(cmd, name))
}

// MustGetBool returns the bool value of a flag with the given name and panics
// if that flag was never defined.
func MustGetBool(cmd *cobra.Command, name string) bool {
	value, err := cmd.Flags().GetBool(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetBoolSlice returns the []bool value of a flag with the given name and
// panics if that flag was never defined.
func MustGetBoolSlice(cmd *cobra.Command, name string) []bool {
	value, err := cmd.Flags().GetBoolSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetBytesBase64 returns the []byte value of a flag with the given name and
// panics if that flag was never defined.
func MustGetBytesBase64(cmd *cobra.Command, name string) []byte {
	value, err := cmd.Flags().GetBytesBase64(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetBytesHex returns the []byte value of a flag with the given name and
// panics if that flag was never defined.
func MustGetBytesHex(cmd *cobra.Command, name string) []byte {
	value, err := cmd.Flags().GetBytesHex(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetCount returns the int value of a flag with the given name and panics
// if that flag was never defined.
func MustGetCount(cmd *cobra.Command, name string) int {
	value, err := cmd.Flags().GetCount(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetDuration returns the time.Duration of a flag with the given name and
// panics if that flag was never defined.
func MustGetDuration(cmd *cobra.Command, name string) time.Duration {
	value, err := cmd.Flags().GetDuration(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetDurationSlice returns the []time.Duration of a flag with the given
// name and panics if that flag was never defined.
func MustGetDurationSlice(cmd *cobra.Command, name string) []time.Duration {
	value, err := cmd.Flags().GetDurationSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetFloat32 returns the float32 value of a flag with the given name and
// panics if that flag was never defined.
func MustGetFloat32(cmd *cobra.Command, name string) float32 {
	value, err := cmd.Flags().GetFloat32(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetFloat32Slice returns the []float32 value of a flag with the given name
// and panics if that flag was never defined.
func MustGetFloat32Slice(cmd *cobra.Command, name string) []float32 {
	value, err := cmd.Flags().GetFloat32Slice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetFloat64 returns the float64 value of a flag with the given name and
// panics if that flag was never defined.
func MustGetFloat64(cmd *cobra.Command, name string) float64 {
	value, err := cmd.Flags().GetFloat64(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetFloat64Slice returns the []float64 value of a flag with the given name
// and panics if that flag was never defined.
func MustGetFloat64Slice(cmd *cobra.Command, name string) []float64 {
	value, err := cmd.Flags().GetFloat64Slice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetIP returns the net.IP value of a flag with the given name and panics
// if that flag was never defined.
func MustGetIP(cmd *cobra.Command, name string) net.IP {
	value, err := cmd.Flags().GetIP(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetIPNet returns the net.IPNet value of a flag with the given name and
// panics if that flag was never defined.
func MustGetIPNet(cmd *cobra.Command, name string) net.IPNet {
	value, err := cmd.Flags().GetIPNet(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetIPSlice returns the []net.IP value of a flag with the given name and
// panics if that flag was never defined.
func MustGetIPSlice(cmd *cobra.Command, name string) []net.IP {
	value, err := cmd.Flags().GetIPSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetIPv4Mask returns the net.IPMask value of a flag with the given name
// and panics if that flag was never defined.
func MustGetIPv4Mask(cmd *cobra.Command, name string) net.IPMask {
	value, err := cmd.Flags().GetIPv4Mask(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt returns the int value of a flag with the given name and panics if
// that flag was never defined.
func MustGetInt(cmd *cobra.Command, name string) int {
	value, err := cmd.Flags().GetInt(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt16 returns the int16 value of a flag with the given name and panics if
// that flag was never defined.
func MustGetInt16(cmd *cobra.Command, name string) int16 {
	value, err := cmd.Flags().GetInt16(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt32 returns the int32 value of a flag with the given name and panics if
// that flag was never defined.
func MustGetInt32(cmd *cobra.Command, name string) int32 {
	value, err := cmd.Flags().GetInt32(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt32Slice returns the []int32 value of a flag with the given name and panics if
// that flag was never defined.
func MustGetInt32Slice(cmd *cobra.Command, name string) []int32 {
	value, err := cmd.Flags().GetInt32Slice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt64 returns the int64 value of a flag with the given name and panics
// if that flag was never defined.
func MustGetInt64(cmd *cobra.Command, name string) int64 {
	value, err := cmd.Flags().GetInt64(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt64Slice returns the []int64 value of a flag with the given name and panics if
// that flag was never defined.
func MustGetInt64Slice(cmd *cobra.Command, name string) []int64 {
	value, err := cmd.Flags().GetInt64Slice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetInt8 returns the int8 value of a flag with the given name and panics
// if that flag was never defined.
func MustGetInt8(cmd *cobra.Command, name string) int8 {
	value, err := cmd.Flags().GetInt8(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetIntSlice returns the []int value of a flag with the given name and
// panics if that flag was never defined.
func MustGetIntSlice(cmd *cobra.Command, name string) []int {
	value, err := cmd.Flags().GetIntSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetString returns the string value of a flag with the given name and
// panics if that flag was never defined.
func MustGetString(cmd *cobra.Command, name string) string {
	value, err := cmd.Flags().GetString(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetStringSlice returns the []string value of a flag with the given name
// and panics if that flag was never defined.
func MustGetStringSlice(cmd *cobra.Command, name string) []string {
	value, err := cmd.Flags().GetStringSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetStringSlice returns the []string value of a flag with the given name,
// calls os.ExpandEnv on values, and panics if that flag was never defined.
func MustGetStringSliceExpanded(cmd *cobra.Command, name string) []string {
	slice := MustGetStringSlice(cmd, name)
	for i, str := range slice {
		slice[i] = os.ExpandEnv(str)
	}
	return slice
}

// MustGetStringToInt returns the map[string]int value of a flag with the given
// name and panics if that flag was never defined.
func MustGetStringToInt(cmd *cobra.Command, name string) map[string]int {
	value, err := cmd.Flags().GetStringToInt(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetStringToInt64 returns the map[string]int64 value of a flag with the
// given name and panics if that flag was never defined.
func MustGetStringToInt64(cmd *cobra.Command, name string) map[string]int64 {
	value, err := cmd.Flags().GetStringToInt64(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetStringToString returns the map[string]string value of a flag with the
// given name and panics if that flag was never defined.
func MustGetStringToString(cmd *cobra.Command, name string) map[string]string {
	value, err := cmd.Flags().GetStringToString(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUint returns the uint value of a flag with the given name and panics
// if that flag was never defined.
func MustGetUint(cmd *cobra.Command, name string) uint {
	value, err := cmd.Flags().GetUint(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUint16 returns the uint16 value of a flag with the given name and
// panics if that flag was never defined.
func MustGetUint16(cmd *cobra.Command, name string) uint16 {
	value, err := cmd.Flags().GetUint16(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUint32 returns the uint32 value of a flag with the given name and
// panics if that flag was never defined.
func MustGetUint32(cmd *cobra.Command, name string) uint32 {
	value, err := cmd.Flags().GetUint32(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUint64 returns the uint64 value of a flag with the given name and
// panics if that flag was never defined.
func MustGetUint64(cmd *cobra.Command, name string) uint64 {
	value, err := cmd.Flags().GetUint64(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUint8 returns the uint8 value of a flag with the given name and panics
// if that flag was never defined.
func MustGetUint8(cmd *cobra.Command, name string) uint8 {
	value, err := cmd.Flags().GetUint8(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}

// MustGetUintSlice returns the []uint value of a flag with the given name and
// panics if that flag was never defined.
func MustGetUintSlice(cmd *cobra.Command, name string) []uint {
	value, err := cmd.Flags().GetUintSlice(name)
	if err != nil {
		panic("failed to find cobra flag: " + name)
	}
	return value
}
