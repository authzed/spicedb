package cobrautil

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// MarkFlagsHidden is a convenient way to mark flags as hidden in bulk.
func MarkFlagsHidden(flags *pflag.FlagSet, names ...string) error {
	for _, name := range names {
		if err := flags.MarkHidden(name); err != nil {
			return fmt.Errorf("failed to mark flag as hidden: %w", err)
		}
	}
	return nil
}

// NewNamedFlagSets creates a new NamedFlagSets and registers it with the
// provided command.
func NewNamedFlagSets(cmd *cobra.Command) *NamedFlagSets {
	nfs := &NamedFlagSets{}
	nfs.SetUsageTemplate(cmd)
	return nfs
}

// SetUsageTemplate overrides the cobra usage template to include the
// named flags sections.
func (nfs *NamedFlagSets) SetUsageTemplate(cmd *cobra.Command) {
	cobra.AddTemplateFunc(nfs.templateFuncName(), nfs.templateFunc)
	cmd.SetUsageTemplate(nfs.usageTemplate())
}

// NamedFlagSets stores named flag sets in the order of calling FlagSet.
//
// This type is largely adapted from [k8s.io/component-base/cli/flag], but
// modified to be less brittle by integrating with cobra's templates rather
// than entirely overriding UsageFuncs and HelpFuncs.
type NamedFlagSets struct {
	// Order is an ordered list of flag set names.
	Order []string

	// FlagSets stores the flag sets by name.
	FlagSets map[string]*pflag.FlagSet

	// NormalizeNameFunc is the normalize function which used to initialize
	// FlagSets created by NamedFlagSets.
	NormalizeNameFunc func(f *pflag.FlagSet, name string) pflag.NormalizedName

	uniqueID int
}

// templateFuncName generates a random template name so that template, which
// has to be registered globally, isn't overridden by any other NamedFlagSets.
func (nfs *NamedFlagSets) templateFuncName() string {
	if nfs.uniqueID == 0 {
		nfs.uniqueID = rand.Int()
	}
	return fmt.Sprintf("namedFlagSets%d", nfs.uniqueID)
}

func hasVisibleFlags(flags *pflag.FlagSet) bool {
	var found bool
	flags.VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden {
			found = true
		}
	})
	return found
}

func (nfs *NamedFlagSets) AddFlagSets(cmd *cobra.Command) {
	for _, name := range nfs.Order {
		cmd.Flags().AddFlagSet(nfs.FlagSet(name))
	}
}

// FlagSet returns the flag set with the given name and adds it to the
// ordered name list if it is not in there yet.
func (nfs *NamedFlagSets) FlagSet(name string) *pflag.FlagSet {
	if nfs.FlagSets == nil {
		nfs.FlagSets = map[string]*pflag.FlagSet{}
	}
	if _, ok := nfs.FlagSets[name]; !ok {
		flagSet := pflag.NewFlagSet(name, pflag.ExitOnError)
		flagSet.SetNormalizeFunc(pflag.CommandLine.GetNormalizeFunc())
		if nfs.NormalizeNameFunc != nil {
			flagSet.SetNormalizeFunc(nfs.NormalizeNameFunc)
		}
		nfs.FlagSets[name] = flagSet
		nfs.Order = append(nfs.Order, name)
	}
	return nfs.FlagSets[name]
}

// printSections prints the given names flag sets in sections, with the maximal
// given column number.
//
// If cols is zero, lines are not wrapped.
func (nfs *NamedFlagSets) printSections(w io.Writer, cols int) {
	for _, name := range nfs.Order {
		fs := nfs.FlagSets[name]
		if !hasVisibleFlags(fs) {
			continue
		}

		wideFS := pflag.NewFlagSet("", pflag.ExitOnError)
		wideFS.AddFlagSet(fs)

		var zzz string
		if cols > 24 {
			zzz = strings.Repeat("z", cols-24)
			wideFS.Int(zzz, 0, strings.Repeat("z", cols-24))
		}

		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n%s Flags:\n%s", name, wideFS.FlagUsagesWrapped(cols))

		if cols > 24 {
			i := strings.Index(buf.String(), zzz)
			lines := strings.Split(buf.String()[:i], "\n")
			fmt.Fprint(w, strings.Join(lines[:len(lines)-1], "\n"))
			fmt.Fprintln(w)
		} else {
			fmt.Fprint(w, buf.String())
		}
	}
}

func (nfs *NamedFlagSets) templateFunc() string {
	var b strings.Builder
	nfs.printSections(&b, 0)
	return b.String()
}

func (nfs *NamedFlagSets) usageTemplate() string {
	return `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

Available Commands:{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

Additional Commands:{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}
{{` + nfs.templateFuncName() + `}}
Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`
}
