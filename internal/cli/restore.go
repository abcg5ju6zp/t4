package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4/internal/checkpoint"
)

func restoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Inspect and restore from S3 checkpoints",
	}
	cmd.AddCommand(restoreListCmd())
	cmd.AddCommand(restoreCheckpointCmd())
	return cmd
}

func restoreListCmd() *cobra.Command {
	var (
		s3  *s3Flags
		enc *objectStoreEncryptionFlags
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List checkpoints available in S3",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newObjectStoreForCommand(cmd.Context(), s3, enc)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cp := checkpoint.New(logrus.StandardLogger())
			keys, err := cp.ListRemote(ctx, store)
			if err != nil {
				return fmt.Errorf("list checkpoints: %w", err)
			}
			out := cmd.OutOrStdout()
			if len(keys) == 0 {
				fmt.Fprintln(out, "No checkpoints found.")
				return nil
			}
			manifest, _ := cp.ReadManifest(ctx, store)
			fmt.Fprintf(out, "%-72s  %10s  %6s\n", "CHECKPOINT", "REVISION", "TERM")
			for _, key := range keys {
				idx, err := cp.ReadCheckpointIndex(ctx, store, key)
				suffix := ""
				if manifest != nil && manifest.CheckpointKey == key {
					suffix = "  (latest)"
				}
				if err != nil {
					fmt.Fprintf(out, "%-72s  %10s  %6s%s\n", key, "?", "?", suffix)
				} else {
					fmt.Fprintf(out, "%-72s  %10d  %6d%s\n", key, idx.Revision, idx.Term, suffix)
				}
			}
			return nil
		},
	}
	s3 = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	return cmd
}

func restoreCheckpointCmd() *cobra.Command {
	var (
		s3            *s3Flags
		enc           *objectStoreEncryptionFlags
		checkpointKey string
		dataDir       string
	)
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Download a checkpoint to a local data directory",
		Long: `Download an S3 checkpoint to a local data directory so that
't4 run' can start from it on the next boot.

By default the latest checkpoint is used. Pass --checkpoint to restore
from a specific earlier revision (use 't4 restore list' to find keys).

If manifest/latest is missing, truncated, or points at an incomplete
checkpoint, every existing checkpoint is scanned and validated (index,
SST objects, Pebble metadata and coverage); the highest-revision complete
checkpoint is restored, and manifest/latest is repaired with a
conditional write. Candidates with missing objects, checksum failures or
unknown formats are skipped with a logged reason. An explicit --checkpoint
never triggers a scan or a manifest repair.

After this command succeeds, start the node with:

  t4 run --data-dir <data-dir> [--s3-bucket <bucket>] ...

Configure --s3-bucket to a different prefix than the source so the
restored node writes to its own namespace and does not overwrite the
original cluster's data. To stay at the restored revision without
replaying newer WAL from S3, omit --s3-bucket entirely.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pebbleDir := filepath.Join(dataDir, "db")
			if _, err := os.Stat(pebbleDir); err == nil {
				return fmt.Errorf("data directory %q already contains a Pebble database; remove it first", pebbleDir)
			}

			store, err := newObjectStoreForCommand(cmd.Context(), s3, enc)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cp := checkpoint.New(logrus.StandardLogger())

			key := checkpointKey
			if key == "" {
				// Default: use manifest/latest. If it is missing, truncated, or
				// otherwise unusable, scan every checkpoint, verify index/SSTs/
				// Pebble metadata, and take the highest-revision complete one —
				// then repair manifest/latest conditionally. An explicit
				// --checkpoint skips both scan and repair.
				status, err := cp.ReadManifestStatus(ctx, store)
				if err != nil {
					return fmt.Errorf("read manifest: %w", err)
				}
				switch {
				case status.Manifest != nil:
					key = status.Manifest.CheckpointKey
					logrus.Infof("using latest checkpoint: %s (rev=%d)", key, status.Manifest.Revision)
				default:
					stagingDir := pebbleDir + ".recover"
					cand, skipped, err := cp.RecoverLatest(ctx, store, nil, stagingDir)
					if err != nil {
						return fmt.Errorf("scan checkpoints: %w", err)
					}
					if cand == nil {
						if status.Corrupt {
							return fmt.Errorf("manifest/latest is unusable (%s) and no complete checkpoint was found", status.Reason)
						}
						return fmt.Errorf("no checkpoints found in s3://%s/%s", s3.Bucket, s3.Prefix)
					}
					for _, rj := range skipped {
						logrus.Warnf("skipping checkpoint %s: %s", rj.IndexKey, rj.Reason)
					}
					if err := os.Rename(stagingDir, pebbleDir); err != nil {
						return fmt.Errorf("install recovered checkpoint: %w", err)
					}
					m := cand.Manifest()
					outcome, err := cp.RepairManifest(ctx, store, m, func(cur *checkpoint.Manifest) error {
						return cp.VerifyCheckpointComplete(ctx, store, nil, cur.CheckpointKey)
					})
					if err != nil {
						return fmt.Errorf("repair manifest after recovery: %w", err)
					}
					if outcome.Written {
						logrus.Infof("manifest/latest repaired to %s", cand.IndexKey)
					} else {
						logrus.Infof("manifest/latest already points to a valid newer checkpoint; left untouched")
					}
					out := cmd.OutOrStdout()
					fmt.Fprintf(out, "Recovered checkpoint\n")
					fmt.Fprintf(out, "  key:       %s\n", cand.IndexKey)
					fmt.Fprintf(out, "  revision:  %d\n", cand.Index.Revision)
					fmt.Fprintf(out, "  term:      %d\n", cand.Index.Term)
					fmt.Fprintf(out, "  data-dir:  %s\n", dataDir)
					fmt.Fprintf(out, "  skipped:   %d incomplete candidate(s)\n", len(skipped))
					fmt.Fprintf(out, "\nStart the restored node:\n")
					fmt.Fprintf(out, "  t4 run --data-dir %s [--s3-bucket <new-bucket>] --listen 0.0.0.0:3379\n", dataDir)
					return nil
				}
			}

			logrus.Infof("restoring checkpoint %q → %s", key, pebbleDir)
			term, rev, err := cp.Restore(ctx, store, key, pebbleDir)
			if err != nil {
				return fmt.Errorf("restore checkpoint: %w", err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Restored checkpoint\n")
			fmt.Fprintf(out, "  key:       %s\n", key)
			fmt.Fprintf(out, "  revision:  %d\n", rev)
			fmt.Fprintf(out, "  term:      %d\n", term)
			fmt.Fprintf(out, "  data-dir:  %s\n", dataDir)
			fmt.Fprintf(out, "\nStart the restored node:\n")
			fmt.Fprintf(out, "  t4 run --data-dir %s [--s3-bucket <new-bucket>] --listen 0.0.0.0:3379\n", dataDir)
			return nil
		},
	}
	s3 = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "checkpoint key to restore (default: latest; use 't4 restore list' to find keys)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "local directory to restore into (required; must not already contain a Pebble database) (env: T4_DATA_DIR)")
	cmd.MarkFlagRequired("data-dir")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{"data-dir": "T4_DATA_DIR"})
	})
	return cmd
}
