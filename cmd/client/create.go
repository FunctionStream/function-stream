package client

import (
	"fmt"
	"github.com/functionstream/function-stream/cmd/client/common"
	"github.com/spf13/cobra"
	"os"
)

func NewCreateCmd() *cobra.Command {
	filePath := ""
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			if filePath == "" {
				return fmt.Errorf("file is required")
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}
			cmd.SilenceUsage = true
			resources, err := DecodeResource(data)
			if err != nil {
				return fmt.Errorf("failed to decode resources: %w", err)
			}

			// TODO: support dry run

			apiCLi := common.GetApiClient()

			hasErr := false
			for _, r := range resources {
				if err := apiCLi.GenericService(r.Kind).Create(cmd.Context(), &r.Spec); err != nil {
					fmt.Printf("Failed to create resource %s: %v\n", r.Metadata.Name, err)
					hasErr = true
					continue
				}
				fmt.Printf("Resource %s/%s created\n", r.Kind, r.Metadata.Name)
			}
			if hasErr {
				return fmt.Errorf("some resources failed to create")
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&filePath, "file", "f", "", "The path to the resources")
	return cmd
}
