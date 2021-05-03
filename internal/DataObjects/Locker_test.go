
/*
 * Copyright (c) Marco Tusa 2021 - present
 *                     GNU GENERAL PUBLIC LICENSE
 *                        Version 3, 29 June 2007
 *
 *  Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 *  Everyone is permitted to copy and distribute verbatim copies
 *  of this license document, but changing it is not allowed.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package DataObjects

import (
	"reflect"
	"testing"
)

func TestLockerImpl_findLock(t *testing.T) {
	//log.SetLevel(log.DebugLevel)

	var tests = []lockerRule{}
	nodes := make(map[string]ProxySQLNodeImpl)
	locker := testLockerFactory()

	tests = rulesTestFindLock(locker)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
				nodes[tt.proxysqlNode1.Dns] = tt.proxysqlNode1
				nodes[tt.proxysqlNode2.Dns] = tt.proxysqlNode2


			if _,got := locker.findLock(nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf(" %s findLock() = %v, want %v",tt.name, got, tt.want)
			}
		})
	}
}
