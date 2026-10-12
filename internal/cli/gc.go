package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/gc"
)

func gcCmd() *cobra.Command {
	var (
		s3   *s3Flags
		enc  *objectStoreEncryptionFlags
		keep int
	)
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Garbage-collect old checkpoints, orphan SSTs, and stale WAL segments from S3",
		Long: `Remove objects from S3 that are no longer needed for recovery or active branches.

GC runs as a journaled, resumable operation: when it starts it pins the current
manifest, the retained-checkpoint/protected-SST set, active branch registrations
and the WAL sequence horizon, then persists a per-item plan (checkpoint
directories, candidate SSTs, WAL segments) under the "gc/" prefix. If the
process exits, a request times out or the command is cancelled, the next run
verifies each ambiguous delete against the object's existence and retries only
the items the pinned plan still lists as unconfirmed. Objects a newer manifest,
a newly registered branch, the live database or followers still need are never
removed.

Three phases are performed in reference order:
  1. Checkpoint GC  — deletes old checkpoint archives, keeping the most recent --keep.
                      Checkpoints pinned by active branch registrations are never deleted.
  2. Orphan SST GC  — deletes SST files that were exclusively referenced by deleted checkpoints.
  3. WAL segment GC — deletes WAL segments whose entire revision range is covered by the
                      latest surviving checkpoint.

Use "t4 gc status" to inspect the active and last finished run.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newObjectStoreForCommand(cmd.Context(), s3, enc)
			if err != nil {
				return err
			}
			ctx := cmd.Context()

			cp := checkpoint.New(logrus.StandardLogger())
			runner := gc.New(cp, store, gc.Options{
				Kind: gc.KindCLI,
				Keep: keep,
				WALHorizon: func(ctx context.Context) (int64, bool, error) {
					manifest, err := cp.ReadManifest(ctx, store)
					if err != nil {
						return 0, false, fmt.Errorf("read manifest: %w", err)
					}
					if manifest == nil {
						// No manifest yet: keep the legacy behavior of
						// skipping WAL segment GC.
						return 0, false, nil
					}
					return manifest.LastSequence, true, nil
				},
				Log: logrus.StandardLogger(),
			})

			res, err := runner.Run(ctx)
			if err != nil {
				if errors.Is(err, gc.ErrFenced) {
					return fmt.Errorf("gc: stopped because another leadership generation owns the active run")
				}
				return fmt.Errorf("gc: %w", err)
			}
			c := res.Counts()
			fmt.Printf("GC complete\n")
			if res.ID != "" {
				fmt.Printf("  run: %s (%s)\n", res.ID, res.Status)
			}
			fmt.Printf("  checkpoints deleted: %d\n", c.CheckpointsDeleted)
			fmt.Printf("  orphan SSTs deleted: %d\n", c.SSTsDeleted)
			fmt.Printf("  WAL segments deleted: %d\n", c.WALsDeleted)
			if c.Retained > 0 {
				fmt.Printf("  items retained: %d\n", c.Retained)
			}
			if res.Status == gc.StatusFailed {
				fmt.Printf("  error: %s (%d item(s) unconfirmed; rerun to resume)\n", res.Error, c.Pending)
				return fmt.Errorf("gc incomplete: %s", res.Error)
			}
			return nil
		},
	}
	s3 = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	cmd.Flags().IntVar(&keep, "keep", 3, "number of most-recent checkpoints to retain")
	cmd.AddCommand(gcStatusCmd())
	return cmd
}

func gcStatusCmd() *cobra.Command {
	var (
		s3  *s3Flags
		enc *objectStoreEncryptionFlags
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the active and most recent GC run",
		Long: `Show the GC run currently in progress (or stopped before completion and
waiting for resume) and the most recent finished run. Reads only the "gc/"
journal in object storage; no running node is required.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newObjectStoreForCommand(cmd.Context(), s3, enc)
			if err != nil {
				return err
			}
			ctx := cmd.Context()

			active, err := gc.ReadActive(ctx, store)
			if err != nil {
				return fmt.Errorf("read active gc run: %w", err)
			}
			latest, err := gc.ReadLatest(ctx, store)
			if err != nil {
				return fmt.Errorf("read latest gc result: %w", err)
			}
			if active == nil && latest == nil {
				fmt.Println("No GC runs recorded.")
				return nil
			}
			if active != nil {
				fmt.Printf("Active run\n")
				printRun(active)
			}
			if latest != nil {
				if active != nil && latest.ID == active.ID {
					return nil
				}
				fmt.Printf("Latest finished run\n")
				printRun(latest)
			}
			return nil
		},
	}
	s3 = addS3Flags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	return cmd
}

func printRun(r *gc.Run) {
	c := r.Counts()
	fmt.Printf("  id:        %s\n", r.ID)
	fmt.Printf("  kind:      %s\n", r.Kind)
	if r.Term != 0 {
		fmt.Printf("  term:      %d\n", r.Term)
	}
	fmt.Printf("  status:    %s\n", r.Status)
	fmt.Printf("  started:   %s\n", time.Unix(0, r.CreatedNano).Format(time.RFC3339))
	if r.FinishedNano != 0 {
		fmt.Printf("  finished:  %s\n", time.Unix(0, r.FinishedNano).Format(time.RFC3339))
	}
	fmt.Printf("  deleted:   checkpoints=%d ssts=%d wal=%d\n", c.CheckpointsDeleted, c.SSTsDeleted, c.WALsDeleted)
	if c.Retained > 0 || c.Pending > 0 {
		fmt.Printf("  retained:  %d\n", c.Retained)
		fmt.Printf("  pending:   %d\n", c.Pending)
	}
	if r.Error != "" {
		fmt.Printf("  error:     %s\n", r.Error)
	}
}
