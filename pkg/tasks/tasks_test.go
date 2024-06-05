package tasks

import (
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	taskTypes "github.com/conductorone/baton-sdk/pkg/types/tasks"
)

func TestIs(t *testing.T) {
	type args struct {
		task   *v1.Task
		target taskTypes.TaskType
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
				target: taskTypes.FullSyncType,
			},
			want: false,
		},
		{
			name: "nil task type",
			args: args{
				task: &v1.Task{
					TaskType: nil,
				},
				target: taskTypes.FullSyncType,
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
				target: taskTypes.FullSyncType,
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
				target: taskTypes.GrantType,
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
				target: taskTypes.RevokeType,
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
				target: taskTypes.HelloType,
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
				target: taskTypes.NoneType,
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
		want taskTypes.TaskType
	}{
		{
			name: "nil task",
			args: args{
				task: nil,
			},
			want: taskTypes.UnknownType,
		},
		{
			name: "nil task type",
			args: args{
				task: &v1.Task{
					TaskType: nil,
				},
			},
			want: taskTypes.UnknownType,
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
			want: taskTypes.FullSyncType,
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
			want: taskTypes.GrantType,
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
			want: taskTypes.RevokeType,
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
			want: taskTypes.HelloType,
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
			want: taskTypes.NoneType,
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
