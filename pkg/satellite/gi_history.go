// SPDX-License-Identifier: Apache-2.0

/*
Copyright 2026 Cozystack contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package satellite

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	intent "github.com/cozystack/blockstor/pkg/satellite/intent"
)

// giHistorySuffix names the per-resource file, in the state directory,
// that records the generation identifiers this satellite has seen its
// replica's volumes carry. It is the per-volume record "this replica held
// established metadata" that repairUncleanMetadata decides on. The Resource
// status is not that record: its current GI is dropped whenever the volume
// leaves a data-bearing disk state, which is exactly when a crash leaves it.
const giHistorySuffix = ".gi-history"

// giHistoryPerVolume bounds how many generations are kept per volume. A
// replica that crashed has its last few in the metadata, and those are the
// ones a check needs.
const giHistoryPerVolume = 32

// RecordObservedGI notes that volume vol of rdName carried gi. The observer
// calls it for every data-bearing volume it reads a GI of; repeated values
// cost a map lookup, not a write.
func (r *Reconciler) RecordObservedGI(rdName string, vol int32, generation string) {
	if r.cfg.StateDir == "" || generation == "" {
		return
	}

	generation = strings.ToUpper(generation)
	key := rdName + "/" + strconv.Itoa(int(vol))

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastRecordedGI == nil {
		r.lastRecordedGI = map[string]string{}
	}

	if r.lastRecordedGI[key] == generation {
		return
	}

	history, err := r.readGIHistory(rdName)
	if err != nil {
		// Writing over a file that could not be read would replace the
		// record with this one GI.
		slog.Default().Warn("could not read a volume's generation record; not recording", "resource", rdName,
			"volume", vol, "err", err)

		return
	}

	if !slices.Contains(history[vol], generation) {
		history[vol] = append(history[vol], generation)
		if extra := len(history[vol]) - giHistoryPerVolume; extra > 0 {
			history[vol] = history[vol][extra:]
		}

		err = r.writeGIHistory(rdName, history)
		if err != nil {
			// Not fatal to the observation, but the record is what lets a
			// crashed replica keep its metadata, so a state directory that
			// cannot be written has to show.
			slog.Default().Warn("could not record a volume's generation; a crash of this replica "+
				"would re-initialise its metadata", "resource", rdName, "volume", vol, "err", err)

			return
		}
	}

	r.lastRecordedGI[key] = generation
}

// keepGIHistoryOf drops the record of every volume of rdName that is not in
// vols, the volumes the definition has now. A volume removed and added again
// under the same number is a new volume: it must not inherit the old one's
// generations, or a lower disk still carrying the old superblock would be
// kept rather than initialised.
func (r *Reconciler) keepGIHistoryOf(rdName string, vols []int32) {
	if r.cfg.StateDir == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	history, err := r.readGIHistory(rdName)
	if err != nil {
		slog.Default().Warn("could not read a generation record; keeping it as it is", "resource", rdName, "err", err)

		return
	}

	dropped := false

	for vol := range history {
		if !slices.Contains(vols, vol) {
			delete(history, vol)
			delete(r.lastRecordedGI, rdName+"/"+strconv.Itoa(int(vol)))

			dropped = true
		}
	}

	if !dropped {
		return
	}

	err = r.writeGIHistory(rdName, history)
	if err != nil {
		slog.Default().Warn("could not drop the generations of removed volumes", "resource", rdName, "err", err)
	}
}

// recordedGIs is what RecordObservedGI kept for volume vol of rdName.
func (r *Reconciler) recordedGIs(rdName string, vol int32) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	history, err := r.readGIHistory(rdName)
	if err != nil {
		return nil, err
	}

	return history[vol], nil
}

func (r *Reconciler) giHistoryPath(rdName string) string {
	return filepath.Join(r.cfg.StateDir, rdName+giHistorySuffix)
}

// readGIHistory reads the file. A missing one is an empty history, which
// leaves the volume to be initialised as before; one that cannot be read is
// an error, never an empty history, since an empty one is what lets the
// repair re-initialise a volume that has a record.
func (r *Reconciler) readGIHistory(rdName string) (map[int32][]string, error) {
	out := map[int32][]string{}

	data, err := os.ReadFile(r.giHistoryPath(rdName))
	if os.IsNotExist(err) {
		return out, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read the generation record of %s: %w", rdName, err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		volField, generation, ok := strings.Cut(strings.TrimSpace(scanner.Text()), " ")
		if !ok || generation == "" {
			continue
		}

		vol, convErr := strconv.ParseInt(volField, 10, 32)
		if convErr != nil {
			continue
		}

		out[int32(vol)] = append(out[int32(vol)], generation)
	}

	return out, nil
}

func (r *Reconciler) writeGIHistory(rdName string, history map[int32][]string) error {
	vols := make([]int32, 0, len(history))
	for vol := range history {
		vols = append(vols, vol)
	}

	slices.Sort(vols)

	var buf bytes.Buffer

	for _, vol := range vols {
		for _, gi := range history[vol] {
			buf.WriteString(strconv.Itoa(int(vol)) + " " + gi + "\n")
		}
	}

	return writeFileDurably(r.giHistoryPath(rdName), buf.Bytes())
}

// forgetGIHistory drops the record of rdName and its cached last GIs, so a
// replica created again under the name starts with no record and its first
// GI is written rather than taken for one already on file. It holds the
// lock RecordObservedGI writes under, so a write already under way finishes
// first. An observation of the old replica processed after this one would
// still write a fresh file; by then the device is down and the observer has
// no GI left to read, so the window is an event already in its queue.
func (r *Reconciler) forgetGIHistory(rdName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	prefix := rdName + "/"
	for key := range r.lastRecordedGI {
		if strings.HasPrefix(key, prefix) {
			delete(r.lastRecordedGI, key)
		}
	}

	err := os.Remove(r.giHistoryPath(rdName))
	if err != nil && !os.IsNotExist(err) {
		return err //nolint:wrapcheck // the path is in the error
	}

	return nil
}

// writeFileDurably replaces path with data so that a power loss leaves
// either the old content or the new: the record exists for a node that went
// down hard, and the GI it most needs is the one written just before.
func writeFileDurably(path string, data []byte) error {
	tmp := path + ".tmp"

	err := writeAndSync(tmp, data)
	if err != nil {
		return errors.Join(err, os.Remove(tmp))
	}

	err = os.Rename(tmp, path)
	if err != nil {
		return errors.Join(err, os.Remove(tmp))
	}

	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err //nolint:wrapcheck // best-effort record; the caller drops the error
	}

	return errors.Join(dir.Sync(), dir.Close())
}

func writeAndSync(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, resFilePerm)
	if err != nil {
		return err //nolint:wrapcheck // best-effort record; the caller drops the error
	}

	_, err = file.Write(data)

	return errors.Join(err, file.Sync(), file.Close())
}

// desiredVolumeNumbers lists the volume numbers dr asks for.
func desiredVolumeNumbers(dr *intent.DesiredResource) []int32 {
	out := make([]int32, 0, len(dr.GetVolumes()))
	for _, vol := range dr.GetVolumes() {
		out = append(out, vol.GetVolumeNumber())
	}

	return out
}
