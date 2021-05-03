
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

package Proxy

/*
Class dealing with ALL SQL commands.
No SQL should be hardcoded in the any other class
*/

const (
	//get MySQL nodes to process based on the HG
	//+--------------+---------------+------+-----------+--------------+---------+-------------+-----------------+---------------------+---------+----------------+-------------------------------------------------------------|---------+
	//| hostgroup_id | hostname      | port | gtid_port | status       | weight  | compression | max_connections | max_replication_lag | use_ssl | max_latency_ms | comment                                                     |ConnUsed|
	Dml_Select_mysql_nodes = " select b.*, c.ConnUsed from stats_mysql_connection_pool c left JOIN runtime_mysql_servers b ON  c.hostgroup=b.hostgroup_id and c.srv_host=b.hostname and c.srv_port = b.port where hostgroup_id in (?)  order by hostgroup,srv_host desc;"

	//get Variables
	Dml_show_variables = "SHOW GLOBAL VARIABLES"

	Dml_select_proxy_servers = "select weight,hostname,port,comment from runtime_proxysql_servers order by weight desc"

	Dml_delete_proxy_servers         = "delete from proxysql_servers where hostname = '?1' and port = ?2 "
	Dml_update_comment_proxy_servers = "update proxysql_servers set comment ='?'"
)
