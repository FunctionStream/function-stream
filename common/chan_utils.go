/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import "context"

func SendToChannel[T any](ctx context.Context, c chan<- T, e interface{}) bool {
	select {
	case c <- e.(T): // It will panic if `e` is not of type `T` or a type that can be converted to `T`.
		return true
	case <-ctx.Done():
		close(c)
		return false
	}
}

func zeroValue[T any]() T {
	var v T
	return v
}

func ReceiveFromChannel[T any](ctx context.Context, c <-chan T) (T, bool) {
	select {
	case e := <-c:
		return e, true
	case <-ctx.Done():
		return zeroValue[T](), false
	}
}
