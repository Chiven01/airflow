# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
import shutil
import time
import xml.dom.minidom as xmldom

from airflow.hooks.docker_hook import DockerHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.svn import svnclient
from airflow.configuration import AIRFLOW_TMP, AIRFLOW_SVN, HADOOP_CONF_PATH, HIVE_CONF_PATH, SPARK_CONF_PATH, HIVE_CONF_URL, HADOOP_CONF_URL, UGI_URL, SPARK_CONF_ORIGIN, DEFAULT_IMAGE, EXTRA_HOSTS
from docker import APIClient, tls
import ast


class DockerOperator(BaseOperator):
    """
    Execute a bash_command inside a docker container.

    A temporary directory is created on the host and
    mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container.
    The path to the mounted directory can be accessed
    via the environment variable ``AIRFLOW_TMP_DIR``.

    If a login to a private registry is required prior to pulling the image, a
    Docker connection needs to be configured in Airflow and the connection ID
    be provided with the parameter ``docker_conn_id``.

    :param image: Docker image from which to create the container.
        If image tag is omitted, "latest" will be used.
    :type image: str
    :param api_version: Remote API version. Set to ``auto`` to automatically
        detect the server's version.
    :type api_version: str
    :param auto_remove: Auto-removal of the container on daemon side when the
        container's process exits.
        The default is False.
    :type auto_remove: bool
    :param bash_command: Command to be run in the container. (templated)
    :type bash_command: str or list
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param dns: Docker custom DNS servers
    :type dns: list[str]
    :param dns_search: Docker custom DNS search domain
    :type dns_search: list[str]
    :param docker_url: URL of the host running the docker daemon.
        Default is unix://var/run/docker.sock
    :type docker_url: str
    :param env: Environment variables to set in the container. (templated)
    :type env: dict
    :param force_pull: Pull the docker image on every run. Default is False.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use.
        Either a float value, which represents the limit in bytes,
        or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
    :param host_tmp_dir: Specify the location of the temporary directory on the host which will
        be mapped to tmp_dir. If not provided defaults to using the standard system temp directory.
    :type host_tmp_dir: str
    :param network_mode: Network mode for the container.
    :type network_mode: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority
        to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate
        used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_hostname: Hostname to match against
        the docker server certificate or False to disable the check.
    :type tls_hostname: str or bool
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param tmp_dir: Mount point inside the container to
        a temporary directory created on the host by the operator.
        The path is also made available via the env variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
    :type volumes: list
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client)
    :type working_dir: str
    :param xcom_push: Does the stdout will be pushed to the next step using XCom.
        The default is False.
    :type xcom_push: bool
    :param xcom_all: Push all the stdout or just the last line.
        The default is False (last line).
    :type xcom_all: bool
    :param docker_conn_id: ID of the Airflow connection to use
    :type docker_conn_id: str
    :param shm_size: Size of ``/dev/shm`` in bytes. The size must be
        greater than 0. If omitted uses system default.
    :type shm_size: int
    """
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)

    @apply_defaults
    def __init__(
            self,
            image=DEFAULT_IMAGE,
            api_version=None,
            bash_command=None,
            cpus=1.0,
            docker_url='unix:///var/run/docker.sock',
            env=None,
            force_pull=False,
            mem_limit=None,
            host_tmp_dir=None,
            network_mode=None,
            tls_ca_cert=None,
            tls_client_cert=None,
            tls_client_key=None,
            tls_hostname=None,
            tls_ssl_version=None,
            tmp_dir='/tmp/airflow',
            user=None,
            volumes=None,
            working_dir=None,
            xcom_push=False,
            xcom_all=False,
            docker_conn_id=None,
            dns=None,
            dns_search=None,
            auto_remove=False,
            shm_size=None,
            svn_url=None,
            *args,
            **kwargs):

        super(DockerOperator, self).__init__(*args, **kwargs)
        self.api_version = '1.39'
        self.auto_remove = True
        self.bash_command = bash_command
        self.cpus = cpus
        self.dns = None
        self.dns_search = None
        self.docker_url = 'unix:///var/run/docker.sock'
        self.env = env or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.host_tmp_dir = AIRFLOW_TMP
        self.network_mode = 'bridge'
        self.tls_ca_cert = None
        self.tls_client_cert = None
        self.tls_client_key = None
        self.tls_hostname = None
        self.tls_ssl_version = None
        self.tmp_dir = tmp_dir
        self.user = user
        self.volumes = []
        self.working_dir = working_dir
        self.xcom_push_flag = False
        self.xcom_all = False
        self.docker_conn_id = None
        self.shm_size = shm_size

        self.cli = None
        self.container = None

        self.svn_url = svn_url

    def get_hook(self):
        return DockerHook(
            docker_conn_id=self.docker_conn_id,
            base_url=self.docker_url,
            version=self.api_version,
            tls=self.__get_tls_config()
        )

    def execute(self, context):
        self.log.info('Starting docker container from image %s', self.image)

        tls_config = self.__get_tls_config()

        if self.docker_conn_id:
            self.cli = self.get_hook().get_conn()
        else:
            self.cli = APIClient(
                base_url=self.docker_url,
                version=self.api_version,
                tls=tls_config
            )

        # json.loads会随机失败，怀疑是因为网络问题两条数据一起生成
        # if self.force_pull or len(self.cli.images(name=self.image)) == 0:
        #     self.log.info('Pulling docker image %s', self.image)
        #     for l in self.cli.pull(self.image, stream=True):
        #         output = json.loads(l.decode('utf-8').strip())
        #         if 'status' in output:
        #             self.log.info("%s", output['status'])

        if self.force_pull or len(self.cli.images(name=self.image)) == 0:
            self.log.info('Pulling docker image %s', self.image)
            for l in self.cli.pull(self.image).split("\r\n"):
                if l:
                    output = json.loads(l)
                    if 'status' in output:
                        self.log.info("%s", output['status'])

        with TemporaryDirectory(prefix='airflowtmp', dir=self.host_tmp_dir) as host_tmp_dir:
            self.env['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))

            # 获取当前环境变量
            local_env = {}
            # 初始化ni 挂载列表
            volumes = []

            # svn任务
            if self.svn_url:
                # pysvn: the first var must convert to str
                checkoutpath = str(AIRFLOW_SVN) + '/' + self.dag.dag_id + '/' + self.task_id
                self.log.debug("local location: {}".format(checkoutpath))
                # 若文件不存在则checkout
                if not os.path.exists(checkoutpath):
                    self.log.info("Check out: {}".format(self.svn_url))
                    svnclient.checkout(self.svn_url, checkoutpath)
                else:
                    entry = svnclient.info(checkoutpath)
                    # 文件存在 但与当前路径不一致
                    if entry.url != self.svn_url:
                        self.log.info("Romove path: {}".format(entry.url))
                        shutil.rmtree(checkoutpath)
                        self.log.info("Check out: {}".format(self.svn_url))
                        svnclient.checkout(self.svn_url, checkoutpath)
                    # 文件存在 且与当前路径一致
                    else:
                        self.log.info("Update path: {}".format(self.svn_url))
                        svnclient.update(checkoutpath)

                # copy待执行文件到临时目录
                shutil.copytree(checkoutpath, str(host_tmp_dir) + '/svn')
                # 添加映射路径
                volumes.append(str(host_tmp_dir) + '/svn:/root/svn')

            # 解析env
            if self.env is not None:
                for key, value in self.env.items():
                    if key != "HADOOP_CONF_DIR" and key != "HIVE_CONF_DIR":
                        local_env[key] = value

            if 'HADOOP_CONF_DIR' in self.env:
                hadoop_conf_path = HADOOP_CONF_PATH + self.env['HADOOP_CONF_DIR']
                local_env["HADOOP_CONF_DIR"] = hadoop_conf_path
                for i in range(3):
                    try:
                        if os.path.exists(hadoop_conf_path):
                            svnclient.update(hadoop_conf_path)
                        else:
                            svnclient.checkout(HADOOP_CONF_URL + self.env['HADOOP_CONF_DIR'], hadoop_conf_path)
                        break
                    except Exception:
                        self.log.info("Update hadoop Failed: {}".format(self.env['HADOOP_CONF_DIR']))
                        self.log.info("Sleep 10s to restart")
                        time.sleep(10)
                        if i == 2:
                            raise Exception("Update hadoop Failed")
                        else:
                            continue

                ugi_path = ''
                ugi_dir = ''
                for i in range(3):
                    ugi_file = ''
                    try:
                        xmlfilepath = os.path.abspath(hadoop_conf_path + "/core-site.xml")
                        domobj = xmldom.parse(xmlfilepath)
                        elementobj = domobj.documentElement
                        subelementobj = elementobj.getElementsByTagName("property")
                        for i in range(len(subelementobj)):
                            if subelementobj[i].getElementsByTagName("name")[0].firstChild.data == "hadoop.client.ugi":
                                ugi_file = subelementobj[i].getElementsByTagName("value")[0].firstChild.data
                        ugi_dir = ugi_file.replace("/ugi_config", "").replace("ugi/", "")
                        ugi_path = '~/ugi/' + ugi_dir
                        ugi_path = os.path.expanduser(ugi_path)
                        if os.path.exists(ugi_path):
                            self.log.info("Update ugi path: {}".format(ugi_path))
                            svnclient.update(ugi_path)
                        else:
                            self.log.info(
                                "Check out ugi path from: {}/{} to {}".format(UGI_URL, ugi_dir, ugi_path))
                            svnclient.checkout(UGI_URL + "/" + ugi_dir, ugi_path)
                        break
                    except Exception:
                        self.log.info("Update Failed: {}".format(ugi_file))
                        self.log.info("Sleep 10s to restart")
                        time.sleep(10)
                        if i == 2:
                            raise Exception("Update ugi Failed")
                        else:
                            continue

                # 添加映射路径
                volumes.append(hadoop_conf_path + ":" + HADOOP_CONF_PATH + self.env['HADOOP_CONF_DIR'])
                local_env["HADOOP_CONF_DIR"] = HADOOP_CONF_PATH + self.env['HADOOP_CONF_DIR']

                volumes.append(ugi_path + ":/root/" + ugi_dir)
                self.log.info("volumes: {}".format(volumes))

            if 'HIVE_CONF_DIR' in self.env and self.env['HIVE_CONF_DIR'] != "":
                spark_conf_path = SPARK_CONF_PATH + self.env['HIVE_CONF_DIR']
                local_env["SPARK_CONF_DIR"] = spark_conf_path
                hive_conf_path = HIVE_CONF_PATH + self.env['HIVE_CONF_DIR']
                local_env["HIVE_CONF_DIR"] = hive_conf_path

                for i in range(3):
                    try:
                        if os.path.exists(hive_conf_path):
                            svnclient.update(hive_conf_path)
                        else:
                            svnclient.checkout(HIVE_CONF_URL + self.env['HIVE_CONF_DIR'], hive_conf_path)
                        break
                    except Exception:
                        self.log.info("Update hive Failed: {}".format(self.env['HIVE_CONF_DIR']))
                        self.log.info("Sleep 10s to restart")
                        time.sleep(10)
                        if i == 2:
                            raise Exception("Update hive Failed")
                        else:
                            continue

                if self.env['HIVE_CONF_DIR'] == 'origin':
                    raise Exception("origin is a reserved key word of HIVE_CONF_DIR")
                spark_conf_origin = SPARK_CONF_PATH + 'origin'

                if not os.path.exists(spark_conf_path):
                    for i in range(3):
                        try:
                            if os.path.exists(spark_conf_origin):
                                svnclient.update(spark_conf_origin)
                            else:
                                svnclient.checkout(SPARK_CONF_ORIGIN, spark_conf_origin)
                            #shutil.copytree(spark_conf_origin, spark_conf_path)
                            shutil.copytree("/opt/spark/conf", spark_conf_path)
                            break
                        except Exception:
                            self.log.info("Update spark conf Failed: {}".format(self.env['HIVE_CONF_DIR']))
                            self.log.info("Sleep 10s to restart")
                            time.sleep(10)
                            if i == 2:
                                raise Exception("Update spark Failed")
                            else:
                                continue

                if os.path.exists(spark_conf_path + "/hive-site.xml"):
                    os.remove(spark_conf_path + "/hive-site.xml")
                try:
                    shutil.copy(hive_conf_path + "/hive-site.xml", spark_conf_path)
                except Exception:
                    self.log.info("Copy file Failed: {}".format(hive_conf_path + "/hive-site.xml"))

                # 添加映射路径
                volumes.append(hive_conf_path + ":" + HIVE_CONF_PATH + self.env['HIVE_CONF_DIR'])
                local_env["HIVE_CONF_DIR"] = HIVE_CONF_PATH + self.env['HIVE_CONF_DIR']

                # 添加映射路径
                volumes.append(spark_conf_path + ":" + SPARK_CONF_PATH + self.env['HIVE_CONF_DIR'])
                local_env["SPARK_CONF_DIR"] = SPARK_CONF_PATH + self.env['HIVE_CONF_DIR']

            self.log.info("%s", volumes)
            self.log.info("%s", local_env)
            self.log.info("%s", self.bash_command)

            
            self.container = self.cli.create_container(
                command=self.bash_command,
                environment=local_env,
                host_config=self.cli.create_host_config(
                    auto_remove=self.auto_remove,
                    binds=volumes,
                    network_mode=self.network_mode,
                    extra_hosts=EXTRA_HOSTS,
                    shm_size=self.shm_size,
                    dns=self.dns,
                    dns_search=self.dns_search,
                    cpu_shares=int(round(self.cpus * 1024)),
                    mem_limit=self.mem_limit),
                image=self.image,
                user=self.user,
                working_dir="/root/svn"
            )
            self.cli.start(self.container['Id'])

            line = ''
            for line in self.cli.logs(container=self.container['Id'],
                                      stdout=True,
                                      stderr=True,
                                      stream=True):
                line = line.strip()
                if hasattr(line, 'decode'):
                    line = line.decode('utf-8')
                self.log.info(line)

            result = self.cli.wait(self.container['Id'])
            if result['StatusCode'] != 0:
                raise AirflowException('docker container failed: ' + repr(result))

            if self.xcom_push_flag:
                return self.cli.logs(container=self.container['Id']) \
                    if self.xcom_all else str(line)

    def get_command(self):
        if isinstance(self.bash_command, str) and self.bash_command.strip().find('[') == 0:
            bash_commands = ast.literal_eval(self.bash_command)
        else:
            bash_commands = self.bash_command
        return bash_commands

    def on_kill(self):
        if self.cli is not None:
            self.log.info('Stopping docker container')
            self.cli.stop(self.container['Id'])

    def __get_tls_config(self):
        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,
                assert_hostname=self.tls_hostname
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')
        return tls_config
