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

package Pcx

const (
	/*
	   Retrieve main information from Data nodes
	*/

	Dml_get_variables = "select * from performance_schema.global_variables"
	Dml_get_status    = "select * from performance_schema.global_status"
	Dml_get_pxc_view  = "select * from performance_schema.pxc_cluster_view where UUID = '?'"

	Dml_get_ssl_status = "SHOW SESSION STATUS LIKE 'Ssl_cipher'"
)
