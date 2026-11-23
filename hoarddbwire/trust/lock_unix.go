// Copyright 2026 Boeger IT Sh.p.k
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build unix && !solaris && !aix

package trust

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// flock is a variable so a test can simulate a file system without locks.
var flock = syscall.Flock

// lockTimeout bounds the wait for another holder: the lock is taken inside a
// TLS handshake, which must not hang on a stuck process.
var lockTimeout = 5 * time.Second

const lockPoll = 10 * time.Millisecond

// lockFile takes an advisory exclusive lock so writers in separate processes
// serialise. It polls without blocking until lockTimeout. A file system that
// cannot lock (some FUSE mounts) reports errLockUnsupported.
func lockFile(file *os.File) error {
	deadline := time.Now().Add(lockTimeout)
	for {
		err := flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.ENOLCK), errors.Is(err, syscall.EOPNOTSUPP):
			return errLockUnsupported
		case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EINTR):
			if time.Now().After(deadline) {
				return fmt.Errorf("another process has held the lock for more than %v: %w", lockTimeout, ErrLockTimeout)
			}
			time.Sleep(lockPoll)
		default:
			return err
		}
	}
}

func unlockFile(file *os.File) { _ = flock(int(file.Fd()), syscall.LOCK_UN) }
