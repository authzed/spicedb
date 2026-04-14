package pool

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestMaxRetryError_Error(t *testing.T) {
	type fields struct {
		MaxRetries uint8
		LastErr    error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no retries, no err",
			fields: fields{
				MaxRetries: 0,
				LastErr:    nil,
			},
			want: "retries disabled",
		},
		{
			name: "retries, no err",
			fields: fields{
				MaxRetries: 10,
				LastErr:    nil,
			},
			want: "max retries reached (10)",
		},
		{
			name: "retries, err",
			fields: fields{
				MaxRetries: 10,
				LastErr:    fmt.Errorf("failed"),
			},
			want: "max retries reached (10): failed",
		},
		{
			name: "no retries, err",
			fields: fields{
				MaxRetries: 0,
				LastErr:    fmt.Errorf("failed"),
			},
			want: "retries disabled: failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &MaxRetryError{
				MaxRetries: tt.fields.MaxRetries,
				LastErr:    tt.fields.LastErr,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaxRetryError_GRPCStatus(t *testing.T) {
	type fields struct {
		MaxRetries uint8
		LastErr    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *status.Status
	}{
		{
			name: "no retries, no err",
			fields: fields{
				MaxRetries: 0,
				LastErr:    nil,
			},
			want: nil,
		},
		{
			name: "retries, no err",
			fields: fields{
				MaxRetries: 10,
				LastErr:    nil,
			},
			want: nil,
		},
		{
			name: "retries, err",
			fields: fields{
				MaxRetries: 10,
				LastErr:    fmt.Errorf("failed"),
			},
			want: nil,
		},
		{
			name: "no retries, err",
			fields: fields{
				MaxRetries: 0,
				LastErr:    fmt.Errorf("failed"),
			},
			want: nil,
		},
		{
			name: "retries, grpc status",
			fields: fields{
				MaxRetries: 10,
				LastErr:    status.Error(codes.Unavailable, "failed"),
			},
			want: status.New(codes.Unavailable, "failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &MaxRetryError{
				MaxRetries: tt.fields.MaxRetries,
				LastErr:    tt.fields.LastErr,
			}
			if got := e.GRPCStatus(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GRPCStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaxRetryError_Unwrap(t *testing.T) {
	type fields struct {
		MaxRetries uint8
		LastErr    error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "no retries, no err",
			fields: fields{
				MaxRetries: 0,
			},
			wantErr: false,
		},
		{
			name: "retries, no err",
			fields: fields{
				MaxRetries: 10,
			},
			wantErr: false,
		},
		{
			name: "retries, err",
			fields: fields{
				MaxRetries: 10,
				LastErr:    fmt.Errorf("failed"),
			},
			wantErr: true,
		},
		{
			name: "no retries, err",
			fields: fields{
				MaxRetries: 0,
				LastErr:    fmt.Errorf("failed"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &MaxRetryError{
				MaxRetries: tt.fields.MaxRetries,
				LastErr:    tt.fields.LastErr,
			}
			if err := e.Unwrap(); (err != nil) != tt.wantErr {
				t.Errorf("Unwrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResettableError_Error(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			want: "resettable error",
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			want: "resettable error: failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ResettableError{
				Err: tt.fields.Err,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResettableError_GRPCStatus(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   *status.Status
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			want: status.New(codes.Unavailable, "resettable error"),
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			want: spiceerrors.WithCodeAndDetails(
				fmt.Errorf("failed"),
				codes.Unavailable,
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ResettableError{
				Err: tt.fields.Err,
			}
			if got := e.GRPCStatus(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GRPCStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResettableError_Unwrap(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			wantErr: false,
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &ResettableError{
				Err: tt.fields.Err,
			}
			if err := e.Unwrap(); (err != nil) != tt.wantErr {
				t.Errorf("Unwrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRetryableError_Error(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			want: "retryable error",
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			want: "retryable error: failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &RetryableError{
				Err: tt.fields.Err,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRetryableError_GRPCStatus(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name   string
		fields fields
		want   *status.Status
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			want: status.New(codes.Unavailable, "resettable error"),
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			want: spiceerrors.WithCodeAndDetails(
				fmt.Errorf("failed"),
				codes.Unavailable,
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &RetryableError{
				Err: tt.fields.Err,
			}
			if got := e.GRPCStatus(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GRPCStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRetryableError_Unwrap(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "no err",
			fields: fields{
				Err: nil,
			},
			wantErr: false,
		},
		{
			name: "with err",
			fields: fields{
				Err: fmt.Errorf("failed"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &RetryableError{
				Err: tt.fields.Err,
			}
			if err := e.Unwrap(); (err != nil) != tt.wantErr {
				t.Errorf("Unwrap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_sqlErrorCode(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "nil",
			args: args{
				err: nil,
			},
			want: "",
		},
		{
			name: "not a pg error",
			args: args{
				err: fmt.Errorf("not a pg error"),
			},
			want: "",
		},
		{
			name: "pg error",
			args: args{
				err: &pgconn.PgError{Code: "4001"},
			},
			want: "4001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlErrorCode(tt.args.err); got != tt.want {
				t.Errorf("sqlErrorCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
