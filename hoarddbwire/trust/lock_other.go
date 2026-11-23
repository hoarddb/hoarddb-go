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

//go:build !unix || solaris || aix

package trust

import "os"

// This platform has no flock in the standard syscall package. The per-path mutex
// in Add still serialises this process and Add still re-reads before it writes; only
// writers in separate processes are not serialised.
func lockFile(*os.File) error { return errLockUnsupported }

func unlockFile(*os.File) {}
