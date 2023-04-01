package tasks

import (
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
)

func TestIs(t *testing.T) {
	type args struct {
		task   *v1.Task
		target TaskType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "nil task",
			args: args{
				task:   nil,
				target: FullSyncType,
			},
			want: false,
		},
		{
			name: "nil task type",
			args: args{
				task: &v1.Task{
					TaskType: nil,
				},
				target: FullSyncType,
			},
			want: false,
		},
		{
			name: "full sync",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_SyncFull{
						SyncFull: &v1.Task_SyncFullTask{},
					},
				},
				target: FullSyncType,
			},
			want: true,
		},
		{
			name: "grant",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Grant{
						Grant: &v1.Task_GrantTask{},
					},
				},
				target: GrantType,
			},
			want: true,
		},
		{
			name: "revoke",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Revoke{
						Revoke: &v1.Task_RevokeTask{},
					},
				},
				target: RevokeType,
			},
			want: true,
		},
		{
			name: "hello",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Hello{
						Hello: &v1.Task_HelloTask{},
					},
				},
				target: HelloType,
			},
			want: true,
		},
		{
			name: "none",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_None{
						None: &v1.Task_NoneTask{},
					},
				},
				target: NoneType,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Is(tt.args.task, tt.args.target); got != tt.want {
				t.Errorf("Is() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetType(t *testing.T) {
	type args struct {
		task *v1.Task
	}
	tests := []struct {
		name string
		args args
		want TaskType
	}{
		{
			name: "nil task",
			args: args{
				task: nil,
			},
			want: UnknownType,
		},
		{
			name: "nil task type",
			args: args{
				task: &v1.Task{
					TaskType: nil,
				},
			},
			want: UnknownType,
		},
		{
			name: "full sync",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_SyncFull{
						SyncFull: &v1.Task_SyncFullTask{},
					},
				},
			},
			want: FullSyncType,
		},
		{
			name: "grant",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Grant{
						Grant: &v1.Task_GrantTask{},
					},
				},
			},
			want: GrantType,
		},
		{
			name: "revoke",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Revoke{
						Revoke: &v1.Task_RevokeTask{},
					},
				},
			},
			want: RevokeType,
		},
		{
			name: "hello",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_Hello{
						Hello: &v1.Task_HelloTask{},
					},
				},
			},
			want: HelloType,
		},
		{
			name: "none",
			args: args{
				task: &v1.Task{
					TaskType: &v1.Task_None{
						None: &v1.Task_NoneTask{},
					},
				},
			},
			want: NoneType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetType(tt.args.task); got != tt.want {
				t.Errorf("GetType() = %v, want %v", got, tt.want)
			}
		})
	}
}
