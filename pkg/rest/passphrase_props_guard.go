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

package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/cozystack/blockstor/pkg/passphrase"
)

// withPlaintextPassphraseRefusal refuses a write whose body would store the
// LUKS master passphrase in a property, on any door: controller, group,
// definition, replica, volume, node or pool properties, on create or on
// modify. A property is kept in plaintext on its CR, so any dump of the CRs
// for a backup would carry the key to every LUKS volume; the passphrase
// lives in the Secret `encryption create-passphrase` writes, and blockstor
// reads it from nowhere else. Deleting such a property stays allowed: a key
// named in a delete list is a string, not an object key.
func withPlaintextPassphraseRefusal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || !isWriteMethod(r.Method) {
			next.ServeHTTP(w, r)

			return
		}

		body, err := io.ReadAll(r.Body)
		r.Body = replayedBody{Reader: io.MultiReader(bytes.NewReader(body), failingReader{err: err}), closer: r.Body}

		if err == nil {
			if key, found := plaintextPassphraseKeyIn(body); found {
				writeError(w, http.StatusBadRequest, fmt.Sprintf(
					"refusing to store the LUKS master passphrase in property %q: properties are kept in plaintext "+
						"on their resource; run `linstor encryption create-passphrase`, which keeps it in a Secret", key))

				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func isWriteMethod(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// plaintextPassphraseKeyIn finds an object key, at any depth of a JSON body,
// that would carry the master passphrase. A body that is not JSON has none;
// the handler answers it as before.
func plaintextPassphraseKeyIn(body []byte) (string, bool) {
	var doc any

	if json.Unmarshal(body, &doc) != nil {
		return "", false
	}

	return findPlaintextPassphraseKey(doc)
}

func findPlaintextPassphraseKey(node any) (string, bool) {
	switch typed := node.(type) {
	case map[string]any:
		for key, value := range typed {
			if passphrase.IsPlaintextPropKey(key) {
				return key, true
			}

			if found, ok := findPlaintextPassphraseKey(value); ok {
				return found, true
			}
		}
	case []any:
		for _, value := range typed {
			if found, ok := findPlaintextPassphraseKey(value); ok {
				return found, true
			}
		}
	}

	return "", false
}

// replayedBody hands the handler the bytes the guard read, then the error
// the read ended with, and closes the original body.
type replayedBody struct {
	io.Reader

	closer io.Closer
}

func (b replayedBody) Close() error {
	return b.closer.Close() //nolint:wrapcheck // the request body's own error
}

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}

	return 0, io.EOF
}
