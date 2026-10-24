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

package stream

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/cozystack/blockstor/pkg/storage"
)

// ErrStreamDisabled is a fetch from a satellite whose stream TLS is not
// configured. It wraps storage.ErrNotFound, so the caller falls back to a
// blank volume and a DRBD resync, as it does when no peer has the snapshot.
var ErrStreamDisabled = errors.Wrap(storage.ErrNotFound, "the snapshot stream is disabled: no stream TLS configured")

// Client fetches a snapshot stream from a peer satellite over mutual TLS.
type Client struct {
	files TLSFiles
}

// NewClient builds a Client that presents and verifies certificates from
// files. With files not configured every Fetch answers ErrStreamDisabled: the
// stream carries raw volume data, and it is not sent in the clear.
func NewClient(files TLSFiles) *Client {
	return &Client{files: files}
}

// Fetch opens a snapshot stream from peerAddr (host:port form) for
// (rd, snap, vol). The returned ReadCloser is the response body —
// caller MUST Close it to release the underlying TCP connection.
//
// peerAddr is "host:port", e.g. "10.0.0.5:3372". Resolution from a
// node name to an IP is the caller's concern (Node CRD's
// NetInterfaces[].Address).
//
// Returns storage.ErrNotFound when the peer answers 404 (the named
// snapshot doesn't live on that satellite — caller should try
// another peer from snap.Nodes).
func (c *Client) Fetch(ctx context.Context, peerAddr, rd, snap string, vol int32) (io.ReadCloser, error) {
	if c == nil || !c.files.Configured() {
		return nil, ErrStreamDisabled
	}

	tlsConfig, err := c.files.clientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "snapshot-fetch")
	}

	// A transport per fetch, so the certificate and CA are the ones on
	// disk now. Fetches are rare and each moves a whole volume.
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}

	endpoint := url.URL{
		Scheme: "https",
		Host:   peerAddr,
		Path:   PathPrefix + "/" + url.PathEscape(rd) + "/" + url.PathEscape(snap) + "/" + strconv.Itoa(int(vol)),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, "build snapshot-fetch request")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "GET %s", endpoint.String())
	}

	if resp.StatusCode == http.StatusNotFound {
		// Body is small ("storage object not found\n"); drain to
		// release the connection back to the pool, then surface as
		// ErrNotFound so the caller can fall through to the next
		// peer (or to DRBD resync) without parsing the message.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		return nil, errors.Wrapf(storage.ErrNotFound, "peer %s has no snapshot %s/%s", peerAddr, rd, snap)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		return nil, errors.Wrap(
			errors.Newf("peer %s returned %s: %s", peerAddr, resp.Status, string(body)),
			"snapshot-fetch",
		)
	}

	return resp.Body, nil
}

// PeerAddr formats a peer host + port pair into the host:port shape
// Fetch expects. Exposed so callers (materializeVolume's cross-node
// fallback) don't have to know the satellite stream port number.
func PeerAddr(host string) string {
	return fmt.Sprintf("%s:%d", host, Port)
}
