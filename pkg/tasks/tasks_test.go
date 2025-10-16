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
				task:   v1.Task_builder{}.Build(),
				target: taskTypes.FullSyncType,
			},
			want: false,
		},
		{
			name: "full sync",
			args: args{
				task: v1.Task_builder{
					SyncFull: &v1.Task_SyncFullTask{},
				}.Build(),
				target: taskTypes.FullSyncType,
			},
			want: true,
		},
		{
			name: "grant",
			args: args{
				task: v1.Task_builder{
					Grant: &v1.Task_GrantTask{},
				}.Build(),
				target: taskTypes.GrantType,
			},
			want: true,
		},
		{
			name: "revoke",
			args: args{
				task: v1.Task_builder{
					Revoke: &v1.Task_RevokeTask{},
				}.Build(),
				target: taskTypes.RevokeType,
			},
			want: true,
		},
		{
			name: "hello",
			args: args{
				task: v1.Task_builder{
					Hello: &v1.Task_HelloTask{},
				}.Build(),
				target: taskTypes.HelloType,
			},
			want: true,
		},
		{
			name: "none",
			args: args{
				task: v1.Task_builder{
					None: &v1.Task_NoneTask{},
				}.Build(),
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
				task: v1.Task_builder{}.Build(),
			},
			want: taskTypes.UnknownType,
		},
		{
			name: "full sync",
			args: args{
				task: v1.Task_builder{
					SyncFull: &v1.Task_SyncFullTask{},
				}.Build(),
			},
			want: taskTypes.FullSyncType,
		},
		{
			name: "grant",
			args: args{
				task: v1.Task_builder{
					Grant: &v1.Task_GrantTask{},
				}.Build(),
			},
			want: taskTypes.GrantType,
		},
		{
			name: "revoke",
			args: args{
				task: v1.Task_builder{
					Revoke: &v1.Task_RevokeTask{},
				}.Build(),
			},
			want: taskTypes.RevokeType,
		},
		{
			name: "hello",
			args: args{
				task: v1.Task_builder{
					Hello: &v1.Task_HelloTask{},
				}.Build(),
			},
			want: taskTypes.HelloType,
		},
		{
			name: "none",
			args: args{
				task: v1.Task_builder{
					None: &v1.Task_NoneTask{},
				}.Build(),
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
