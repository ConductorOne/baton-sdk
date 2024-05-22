package cli

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type config struct {
	BaseConfig `mapstructure:",squash"`
	Username   string `mapstructure:"username" description:"Username."`
	Password   string `mapstructure:"password" description:"Password."`
	BaseUrl    string `mapstructure:"baseurl" description:"Base URL." defaultValue:"http://localhost:8080"`
	Retries    uint32 `mapstructure:"retries" description:"Max retries." defaultValue:"10"`
	SyncMore   bool   `mapstructure:"sync-more" description:"Sync more." defaultValue:"1"`
	SyncMore2  bool   `mapstructure:"sync-more-2" description:"Sync more 2."`
}

var expectedFlags = []*pflag.Flag{
	{
		Name:  "username",
		Usage: "Username. ($BATON_USERNAME)",
	},
	{
		Name:  "password",
		Usage: "Password. ($BATON_PASSWORD)",
	},
	{
		Name:     "baseurl",
		Usage:    "Base URL. ($BATON_BASEURL)",
		DefValue: "http://localhost:8080",
	},
	{
		Name:     "retries",
		Usage:    "Max retries. ($BATON_RETRIES)",
		DefValue: "10",
	},
	{
		Name:     "sync-more",
		Usage:    "Sync more. ($BATON_SYNC_MORE)",
		DefValue: "true",
	},
	{
		Name:  "sync-more-2",
		Usage: "Sync more 2. ($BATON_SYNC_MORE_2)",
	},
}

func Test_configToCmdFlags(t *testing.T) {
	cmd := &cobra.Command{}
	cfg := &config{}
	err := configToCmdFlags(cmd, cfg)
	if err != nil {
		t.Errorf("configToCmdFlags() error: %v", err)
	}

	for _, flag := range expectedFlags {
		cmdFlag := cmd.Flag(flag.Name)
		if cmdFlag == nil {
			t.Errorf("cmd.Flag(\"%v\") is nil", flag.Name)
			continue
		}
		if cmdFlag.Name != flag.Name {
			t.Errorf("cmd.Flag(\"%v\").Name != %v", cmdFlag.Name, flag.Name)
		}
		if cmdFlag.Usage != flag.Usage {
			t.Errorf("cmd.Flag(\"%v\").Usage != %v", cmdFlag.Name, flag.Usage)
		}
		if flag.DefValue != "" && cmdFlag.DefValue != flag.DefValue {
			t.Errorf("cmd.Flag(\"%v\").DefValue != %v", cmdFlag.Name, flag.DefValue)
		}
	}

	// Make sure a base config flag isn't set
	cmdFlag := cmd.Flag("log-level")
	if cmdFlag != nil {
		t.Errorf("cmd.Flag(log-level) is not nil")
	}
}
