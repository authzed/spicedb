package slogzerolog

import (
	"context"

	"log/slog"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	slogcommon "github.com/samber/slog-common"
)

type Option struct {
	// log level (default: debug)
	// you can use ZeroLogLeveler to retrieve the level from the global zerolog instance or a custom one
	Level slog.Leveler

	// optional: zerolog logger (default: zerolog.Logger)
	Logger *zerolog.Logger
	// optional: don't add timestamp to record
	NoTimestamp bool

	// optional: customize json payload builder
	Converter Converter
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

func (o Option) NewZerologHandler() slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.Logger == nil {
		// should be selected lazily ?
		o.Logger = &log.Logger
	}

	if o.AttrFromContext == nil {
		o.AttrFromContext = []func(ctx context.Context) []slog.Attr{}
	}

	return &ZerologHandler{
		option: o,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

var _ slog.Handler = (*ZerologHandler)(nil)

type ZerologHandler struct {
	option Option
	attrs  []slog.Attr
	groups []string
}

func (h *ZerologHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *ZerologHandler) Handle(ctx context.Context, record slog.Record) error {
	converter := DefaultConverter
	if h.option.Converter != nil {
		converter = h.option.Converter
	}

	level := LogLevels[record.Level]
	fromContext := slogcommon.ContextExtractor(ctx, h.option.AttrFromContext)
	args := converter(h.option.AddSource, h.option.ReplaceAttr, append(h.attrs, fromContext...), h.groups, &record)

	event := h.option.Logger.
		WithLevel(level).
		Ctx(ctx).
		CallerSkipFrame(3)

	if !h.option.NoTimestamp {
		event.Time(zerolog.TimestampFieldName, record.Time)
	}

	event.Fields(args).Msg(record.Message)

	return nil
}

func (h *ZerologHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ZerologHandler{
		option: h.option,
		attrs:  slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *ZerologHandler) WithGroup(name string) slog.Handler {
	// https://cs.opensource.google/go/x/exp/+/46b07846:slog/handler.go;l=247
	if name == "" {
		return h
	}

	return &ZerologHandler{
		option: h.option,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}
