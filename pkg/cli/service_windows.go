//go:build windows

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
	"gopkg.in/yaml.v2"

	"github.com/conductorone/baton-sdk/pkg/logging"
)

func IsService() bool {
	if ok, _ := svc.IsWindowsService(); ok {
		return true
	}
	return false
}

func getExePath(ctx context.Context) (string, error) {
	p, err := filepath.Abs(os.Args[0])
	if err != nil {
		return "", err
	}

	fInfo, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if fInfo.Mode().IsDir() {
		return "", fmt.Errorf("path is a directory")
	}

	if filepath.Ext(p) == "" {
		p += ".exe"
		fi, err := os.Stat(p)
		if err != nil {
			return "", err
		}
		if fi.Mode().IsDir() {
			return "", fmt.Errorf("path is a directory")
		}
	}

	return p, err
}

func startSvc(name string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: fmt.Sprintf("Start the %s service", name),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("starting service")
			return nil
		},
	}
	return cmd
}

func stopSvc(name string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: fmt.Sprintf("Stop the %s service", name),
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("stopping service")
			return nil
		},
	}
	return cmd
}

func interactiveSetup[T any, PtrT *T](name string, outputFilePath, cfg PtrT) error {
	var ret []reflect.StructField
	fields := reflect.VisibleFields(reflect.TypeOf(*cfg))
	for _, field := range fields {
		if _, ok := skipServiceSetupFields[field.Name]; !ok {
			ret = append(ret, field)
		}
	}

	config := make(map[string]interface{})
	for _, field := range ret {
		var input string
		cfgField := field.Tag.Get("mapstructure")
		if cfgField == "" {
			return fmt.Errorf("mapstructure tag is required on config field %s", field.Name)
		}
		switch reflect.ValueOf(cfg).Elem().FieldByName(field.Name).Type() {
		case stringReflectType:
			// Prompt the user for the value and read it from input
			fmt.Printf("Enter %s: ", field.Name)
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				input = scanner.Text()
				break
			}
			config[cfgField] = input

		case boolReflectType:
			// Prompt the user for the value and read it from input
			fmt.Printf("Enter %s: ", field.Name)
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				input = scanner.Text()
				break
			}
			b, err := strconv.ParseBool(input)
			if err != nil {
				return err
			}
			config[cfgField] = b

		case stringSliceReflectType:
			// Prompt the user for the value and read it from input
			fmt.Printf("Enter %s: ", field.Name)
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				input = scanner.Text()
				break
			}
			config[cfgField] = strings.Split(input, ",")

		default:
			fmt.Println("unsupported type for interactive config", field)
			return errors.New("unsupported type for interactive config")
		}
	}

	f, err := os.CreateTemp("", "baton")
	if err != nil {
		return err
	}

	err = yaml.NewEncoder(f).Encode(config)
	if err != nil {
		return err
	}

	fmt.Println("Wrote config file to", f.Name())

	return nil
}

func setupSvc[T any, PtrT *T](name string, cfg PtrT) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setup",
		Short: fmt.Sprintf("Install and configure the %s service", name),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, err := logging.Init(context.Background(), logging.LogFormatJSON, "error")
			if err != nil {
				return err
			}

			l := ctxzap.Extract(ctx)

			exePath, err := getExePath(ctx)
			if err != nil {
				l.Error("Failed to get executable path.", zap.Error(err))
				return err
			}

			svcMgr, err := mgr.Connect()
			if err != nil {
				l.Error("Failed to connect to service manager.", zap.Error(err))
				return err
			}
			defer svcMgr.Disconnect()

			s, err := svcMgr.OpenService(name)
			if err == nil {
				s.Close()
				return fmt.Errorf("%s is already installed as a service", name)
			}

			s, err = svcMgr.CreateService(name, exePath, mgr.Config{DisplayName: name})
			if err != nil {
				l.Error("Failed to create service.", zap.Error(err), zap.String("service_name", name))
				return err
			}
			defer s.Close()

			err = eventlog.InstallAsEventCreate(name, eventlog.Error|eventlog.Warning|eventlog.Info)
			if err != nil {
				l.Error("Failed to install event log source.", zap.Error(err))
				delErr := s.Delete()
				if delErr != nil {
					l.Error("Failed to remove service.", zap.Error(delErr))
					err = errors.Join(err, delErr)
				}
				return err
			}

			l.Info("Successfully installed service.", zap.String("service_name", name))
			return nil
		},
	}
	return cmd
}

func removeSvc(name string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-service",
		Short: fmt.Sprintf("Remove the %s service", name),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, err := logging.Init(context.Background(), logging.LogFormatJSON, "error")
			if err != nil {
				return err
			}

			l := ctxzap.Extract(ctx)

			svcMgr, err := mgr.Connect()
			if err != nil {
				l.Error("Failed to connect to service manager.", zap.Error(err))
				return err
			}
			defer svcMgr.Disconnect()

			s, err := svcMgr.OpenService(name)
			if err != nil {
				l.Error("Failed to open service.", zap.Error(err), zap.String("service_name", name))
				return err
			}
			defer s.Close()

			err = s.Delete()
			if err != nil {
				l.Error("Failed to remove service.", zap.Error(err))
				return err
			}

			err = eventlog.Remove(name)
			if err != nil {
				l.Error("Failed to remove event log source.", zap.Error(err))
				return err
			}

			l.Info("Successfully removed service.", zap.String("service_name", name))
			return nil
		},
	}
	return cmd
}

func additionalCommands[T any, PtrT *T](connectorName string, cfg PtrT) []*cobra.Command {
	return []*cobra.Command{
		startSvc(connectorName),
		stopSvc(connectorName),
		setupSvc(connectorName, cfg),
		removeSvc(connectorName),
	}
}
