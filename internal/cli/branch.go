package cli

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

func branchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "Manage database branches",
	}
	cmd.AddCommand(branchForkCmd())
	cmd.AddCommand(branchUnforkCmd())
	return cmd
}

func branchForkCmd() *cobra.Command {
	var (
		src           *s3Flags
		enc           *objectStoreEncryptionFlags
		branchID      string
		checkpointKey string
	)
	cmd := &cobra.Command{
		Use:   "fork",
		Short: "Register a new branch and print the checkpoint key to use with 't4 run --branch-checkpoint'",
		Long: `Register a new branch in the source store and print the checkpoint index key.

By default the branch is forked from the latest committed checkpoint revision.
Use --checkpoint to fork from a specific older revision instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newObjectStoreForCommand(cmd.Context(), src, enc)
			if err != nil {
				return err
			}
			var cpKey string
			if checkpointKey != "" {
				// Fork from a specific checkpoint rather than the latest.
				cp := checkpoint.New(logrus.StandardLogger())
				if err := cp.RegisterBranch(cmd.Context(), sourceStore, branchID, checkpointKey); err != nil {
					return fmt.Errorf("register branch: %w", err)
				}
				cpKey = checkpointKey
			} else {
				cpKey, err = t4.Fork(cmd.Context(), sourceStore, branchID)
				if err != nil {
					return err
				}
			}
			fmt.Println(cpKey)
			return nil
		},
	}
	src = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required) (env: T4_BRANCH_ID)")
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "fork from this specific checkpoint key instead of the latest revision")
	cmd.MarkFlagRequired("branch-id")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{"branch-id": "T4_BRANCH_ID"})
	})
	return cmd
}

func branchUnforkCmd() *cobra.Command {
	var (
		src            *s3Flags
		enc            *objectStoreEncryptionFlags
		branchID       string
		branchS3Prefix string
		statusOnly     bool
	)
	cmd := &cobra.Command{
		Use:   "unfork",
		Short: "Retire a branch through a recoverable handoff, then release its source registration",
		Long: `Make the branch independent of the source store and only then release its registration.

The branch copies the ancestor SSTs its current checkpoint still references into its own
S3 prefix and publishes a verifiable, fully-local checkpoint; only once that
checkpoint verifies is the source registration released, after which source GC may
reclaim. The operation is idempotent: if object copy, checkpoint publish or
registration update is interrupted, rerun it to resume the same handoff.

--s3-* flags (and T4_S3_* env vars) address the SOURCE store, exactly as for
'branch fork'. --branch-s3-prefix addresses the branch node's own prefix in the
same bucket. Use --status to inspect a handoff without changing anything.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newObjectStoreForCommand(cmd.Context(), src, enc)
			if err != nil {
				return err
			}

			if statusOnly {
				st, err := t4.UnforkStatus(cmd.Context(), sourceStore, nil, branchID)
				if err != nil {
					return err
				}
				printUnforkStatus(st)
				return nil
			}

			if branchS3Prefix == "" {
				return fmt.Errorf("--branch-s3-prefix is required: the branch must publish its independent checkpoint into its own prefix (env: T4_BRANCH_S3_PREFIX)")
			}
			branchCfg := src.config()
			branchCfg.Prefix = branchS3Prefix
			rawBranch, err := object.NewS3StoreFromConfig(cmd.Context(), branchCfg)
			if err != nil {
				return fmt.Errorf("init branch S3: %w", err)
			}
			branchStore, err := enc.wrapStore(rawBranch)
			if err != nil {
				return fmt.Errorf("encryption key: %w", err)
			}

			result, err := t4.Unfork(cmd.Context(), sourceStore, branchStore, branchID)
			if err != nil {
				return err
			}
			printUnforkStatus(result)
			return nil
		},
	}
	src = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required) (env: T4_BRANCH_ID)")
	cmd.Flags().StringVar(&branchS3Prefix, "branch-s3-prefix", "", "S3 prefix of the branch node's own store, where its independent checkpoint is published (env: T4_BRANCH_S3_PREFIX)")
	cmd.Flags().BoolVar(&statusOnly, "status", false, "show the current handoff state without modifying anything")
	cmd.MarkFlagRequired("branch-id")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"branch-id":          "T4_BRANCH_ID",
			"branch-s3-prefix":  "T4_BRANCH_S3_PREFIX",
		})
	})
	return cmd
}

func printUnforkStatus(st *t4.BranchHandoff) {
	fmt.Printf("Branch handoff  %s\n", st.BranchID)
	fmt.Printf("  source registration: %s\n", st.SourceEntryState)
	if st.HandoffID != "" {
		fmt.Printf("  handoff:            %s (%s)\n", st.HandoffID, valueOrDefault(st.Phase, "unknown"))
	}
	if st.AncestorCheckpointKey != "" {
		fmt.Printf("  ancestor checkpoint:   %s\n", st.AncestorCheckpointKey)
	}
	if st.PinnedCheckpointKey != "" {
		fmt.Printf("  pinned checkpoint:    %s\n", st.PinnedCheckpointKey)
	}
	if st.IndependentCheckpointKey != "" {
		fmt.Printf("  independent checkpoint: %s\n", st.IndependentCheckpointKey)
	}
	if st.Phase == "solidifying" {
		fmt.Printf("  ancestor SSTs copied: %d, %d still referenced\n",
			st.CopiedSSTs, st.ReferencedAncestorSSTs)
	}
}
