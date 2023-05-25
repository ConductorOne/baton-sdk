package cli

import (
	"path/filepath"
	"testing"
)

func Test_getConfigPath(t *testing.T) {
	type args struct {
		customPath string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		wantErr bool
	}{
		{
			name: "no custom path",
			args: args{
				customPath: "",
			},
			want:    ".",
			want1:   ".baton",
			wantErr: false,
		},
		{
			name: "custom absolute path with file extension",
			args: args{
				customPath: filepath.FromSlash("/tmp/baton.yaml"),
			},
			want:    filepath.FromSlash("/tmp"),
			want1:   "baton",
			wantErr: false,
		},
		{
			name: "custom absolute path without file extension",
			args: args{
				customPath: filepath.FromSlash("/tmp/baton"),
			},
			wantErr: true,
		},
		{
			name: "custom path with file extension and trailing slash",
			args: args{
				customPath: filepath.FromSlash("/tmp/baton.yaml/"),
			},
			want:    filepath.FromSlash("/tmp"),
			want1:   "baton",
			wantErr: false,
		},
		{
			name: "custom path with file extension with relative path",
			args: args{
				customPath: filepath.FromSlash("./baton.yaml"),
			},
			want:    ".",
			want1:   "baton",
			wantErr: false,
		},
		{
			name: "custom path with file extension with relative path, ../",
			args: args{
				customPath: filepath.FromSlash("../cfg.yaml"),
			},
			want:    "..",
			want1:   "cfg",
			wantErr: false,
		},
		{
			name: "custom path with no file extension with relative path",
			args: args{
				customPath: filepath.FromSlash("../foo/cfg"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := getConfigPath(tt.args.customPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getConfigPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getConfigPath() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getConfigPath() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
