#!/usr/bin/env python3
import hvac
import base64
import binascii
import io
import os
import logging
import tarfile
import shutil
import subprocess
import socket
import unicodedata
from urllib.parse import urlparse
from pyhocon import ConfigFactory

"""
For spawning virtual environment with used libs
you can poetry - python packaging and dependency management.
All settings in ./pyproject.toml. To spawn environment run:
$ poetry shell
"""

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s] %(message)s')
logger = logging.getLogger(__name__)

java_home = os.getenv('JAVA_HOME', '/usr/local/openjdk-11')
java_options = os.getenv('JAVA_OPTS', '')
jar_file = os.getenv('JAR_FILE', 'we.jar')
jar_args = os.getenv('JAR_ARGS', False)

vault_url = os.getenv('VAULT_URL')
vault_base_path = os.getenv('VAULT_PATH')
vault_role_id = os.getenv('VAULT_ROLE_ID')
vault_secret_id = os.getenv('VAULT_SECRET_ID')

namespace = os.getenv('NAMESPACE')
hostname = socket.gethostname()

with_common_config = os.getenv('WITH_COMMON_CONFIG', False)


class ExecutableApp:
    node = 0
    generator = 1


exec_app = ExecutableApp.generator if jar_file.startswith('generators') else ExecutableApp.node


class Vault:

    def __init__(self, url, base_path, role_id, secret_id):
        self.file_list = []
        self.base_path = base_path

        self.client = hvac.Client(url=url)
        self.client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    def list(self, path):
        p = '{base_path}/metadata/{path}'.format(
            base_path=self.base_path, path=path)
        try:
            data = self.client.list(p).get('data').get('keys')
        except Exception as e:
            logger.error('Cannot list vault path: {}\r\n{}'.format(p, e))
        return data

    def read(self, path):
        p = '{base_path}/data/{path}'.format(
            base_path=self.base_path, path=path)
        try:
            c = self.client.read(p).get('data')
            logger.info('Config: {} version: {}'.format(path, c['metadata']['version']))
        except Exception as e:
            logger.error('Cannot get vault path: {}\r\n{}'.format(p, e))
        return c.get('data')

    def walk(self, path):
        # Add slash to the end
        if not path.endswith('/'):
            path = '{path}/'.format(path=path)

        for key in self.list(path):
            if key.endswith('/'):
                p = '{path}{key}'.format(path=path, key=key)
                self.walk(p)
            else:
                self.file_list.append('{path}{key}'.format(path=path, key=key))
        return self.file_list

    def save(self, data, directory='.'):
        for key, val in data.items():
            path = '{directory}/{key}'.format(directory=directory, key=key)
            # Create destination directory
            if not os.path.isdir(directory):
                os.makedirs(directory)

            try:
                # Decode string if Base64
                val = io.BytesIO(base64.b64decode(val.strip().encode(), validate=True))
                logger.info('{path} is base64. Decoding.'.format(path=path))
                with open(path, 'wb') as f:
                    f.write(val.read())
            except (binascii.Error, ValueError) as e:
                if key.endswith(".key"):
                    logger.error(f"Couldn't decode file '{path}'")
                    raise e
                else:
                    with open(path, 'w') as f:
                        f.writelines(val)


def extract_wallet():
    wallet_path = '/opt/configs/wallet.tar.gz.b64'
    if not os.path.exists(wallet_path):
        return
    keys_file_b64 = open(wallet_path, 'r').read()
    keys_file = base64.b64decode(keys_file_b64)
    keys_file_handle = open('/tmp/wallet.tar.gz', 'wb')
    keys_file_handle.write(keys_file)
    keys_file_handle.close()
    tar = tarfile.open('/tmp/wallet.tar.gz')
    tar.extractall(path="/")
    tar.close()


def move_files(src, dst):
    for f in os.listdir(src):
        src_f = os.path.join(src, f)
        if os.path.isfile(src_f):
            dst_f = os.path.join(dst, f)
            os.makedirs(os.path.dirname(dst_f), exist_ok=True)
            shutil.move(src_f, dst)


def clean_data_state(data_dir):
    if os.path.exists(data_dir):
        try:
            logger.info("Removing data-directory '{}'".format(data_dir))
            shutil.rmtree(data_dir)
        except Exception as e:
            logger.exception(e)
    else:
        logger.info("Data-directory '{}' doesn't exist, nothing to delete".format(data_dir))


def get_vault_config(url, base_path, role_id, secret_id):
    v = Vault(url, base_path, role_id, secret_id)

    # Get a config for hostname with the maximum value
    max_hostname = [i for i in v.list(namespace) if i in hostname]
    if max_hostname:
        max_hostname = max(max_hostname)

    if exec_app == ExecutableApp.generator and not max_hostname:
        raise RuntimeError("Unable to get config for hostname")

    if exec_app == ExecutableApp.node:
        for path in ['config', max_hostname]:
            configs = v.read('{}/{}'.format(namespace, path))
            v.save(configs)
        # Read common config
        if with_common_config:
            common_configs = v.read('common')
            v.save(common_configs)
    else:
        configs = v.read('{}/{}'.format(namespace, max_hostname))
        v.save(configs)

    if max_hostname + '/' in v.list(namespace):
        path = '{namespace}/{hostname}/'.format(
            namespace=namespace, hostname=max_hostname)
        for i in v.walk(path):
            v.save(v.read(i), directory=i.split(path)[1])


def find_data_directory(config, data_folder_name):
    base_directory = config.get_string('node.directory', os.getcwd())
    data_directory = config.get_string('node.data-directory', os.path.join(base_directory, data_folder_name))
    if os.path.isdir(data_directory) or not os.path.exists(data_directory):
        logger.info("Found directory for {}: '{}'".format(data_folder_name, data_directory))
        return data_directory
    else:
        raise RuntimeError(
            "Unexpected path for 'node.data-directory/{}' from node's config: '{}'".format(data_folder_name, data_directory))


# Fixes https://security-tracker.debian.org/tracker/CVE-2021-29921
def validate_ip_address(ip_address):
    for octet in ip_address.split('.'):
        if octet != '0' and octet[0] == '0':
            raise RuntimeError(f'Leading zeros are not permitted in {ip_address}')


def get_config_path():
    if os.getenv('CONFIGNAME_AS_HOSTNAME'):
        extract_wallet()
        return '/opt/configs/{}.conf'.format(socket.gethostname())
    elif vault_url and vault_role_id and vault_secret_id:
        logger.info('Validating vault IP address.')
        vault_hostname = urlparse(vault_url).hostname
        vault_ip = socket.gethostbyname(vault_hostname)
        validate_ip_address(vault_ip)
        logger.info('Applying configurations from vault.')
        get_vault_config(vault_url, vault_base_path,
                         vault_role_id, vault_secret_id)
        return 'node.conf'
    elif exec_app == ExecutableApp.node:
        return '/node/node.conf'
    else:
        return '/generator/application.conf'


config_path = get_config_path()


def get_config():
    with open(config_path, 'r', encoding='utf-8') as fd:
        if exec_app == ExecutableApp.node:
            fallback_config_path = '/node/application.conf'
            with open(fallback_config_path, 'r', encoding='utf-8') as ffd:
                config_content = unicodedata.normalize('NFKD', fd.read())
                fallback_config_content = unicodedata.normalize('NFKD', ffd.read())
                fallback_conf = ConfigFactory.parse_string(fallback_config_content,
                                                           os.path.dirname(fallback_config_path), False)
                return ConfigFactory.parse_string(config_content, os.path.dirname(config_path), False).with_fallback(
                    fallback_conf)
        else:
            config_content = unicodedata.normalize('NFKD', fd.read())
            return ConfigFactory.parse_string(config_content, os.path.dirname(config_path), False)


conf = get_config()


def run_snapshot_starter(data_dir, conf):
    is_datadir_empty = not os.path.exists(data_dir) or (os.path.isdir(data_dir) and len(os.listdir(data_dir)) == 0)
    is_genesis_snapshot_based = conf.get_string('node.blockchain.custom.genesis.type', 'plain').lower() == 'snapshot-based'

    if is_datadir_empty and is_genesis_snapshot_based:
        logger.info("Launching SnapshotStarterApp to download the snapshot from the peers")
        snapshot_starter_class_name = 'com.wavesenterprise.SnapshotStarterApp'

        if os.path.isfile('env'):
            with open('env', 'r') as f:
                for line in f:
                    key, value = line.split('=')
                    os.environ[key] = value.strip()
        cmd = ['{}/bin/java'.format(java_home)]
        cmd.extend(['-cp', '{}:/node/lib/*'.format(jar_file), snapshot_starter_class_name])
        cmd.append(config_path)
        logger.info(' '.join(cmd))
        p = subprocess.Popen(cmd)
        p.wait()
        if p.returncode == 0:
            logger.info("SnapshotStarterApp has successfully finished")
        else:
            raise RuntimeError("SnapshotStarterApp has failed")
    else:
        logger.info(
            f"No need to launch SnapshotStarterApp. Datadir is empty: {is_datadir_empty}, genesis is snapshot-based: {is_genesis_snapshot_based}")


def prepare_node(app):
    if app == ExecutableApp.node:
        data_dir = find_data_directory(conf, 'data')
        confidential_data_dir = find_data_directory(conf, 'confidential-data')
        clean_state = os.getenv('CLEAN_STATE', False)

        if clean_state in [True, 'true', 'True']:
            clean_data_state(data_dir)
            clean_data_state(confidential_data_dir)

        run_snapshot_starter(data_dir, conf)


def run_cmd(exec_app, node_class, generator_class, default_options):
    if exec_app == ExecutableApp.generator:
        main_class_name = generator_class
        logger.info("Starting generator............")
    else:
        main_class_name = node_class
        logger.info("Starting node............")

    if os.path.isfile('env'):
        with open('env', 'r') as f:
            for line in f:
                key, value = line.split('=')
                os.environ[key] = value.strip()
    cmd = ['{}/bin/java'.format(java_home)]

    if java_options:
        cmd.extend(java_options.split(' '))

    for prefix, option in default_options:
        if not option in java_options:
            cmd.append(prefix + option)

    cmd.extend(['-cp', '{}:/node/lib/*'.format(jar_file), main_class_name])

    if jar_args:
        cmd.extend(jar_args.split(' '))

    cmd.append(config_path)

    logger.info(' '.join(cmd))
    os.execve(cmd[0], cmd, os.environ)


def is_waves_crypto(config):
    if exec_app == ExecutableApp.node:
        return config.get_string('node.crypto.type', os.getcwd()).lower() == "waves"
    else:
        return True


def validate_crypto(config, config_file):
    waves_wallet_default_path = '/node/wallet.dat'
    waves_crypto = is_waves_crypto(config)
    errors = []

    # Setting default wallet/keystore directory based on used crypto
    default_wallet_path = waves_wallet_default_path

    # Node and generators have different keystore paths
    if exec_app == ExecutableApp.node:
        wallet_path = os.path.abspath(config.get_string('node.wallet.file', default=default_wallet_path))
    else:
        wallet_path = os.path.abspath(config.get_string('generator.accounts.storage', default=default_wallet_path))

    # Keystores are checked differently depending on crypto type
    if waves_crypto:
        if not os.path.exists(wallet_path):
            errors.append("Keystore file '{}' was not found".format(wallet_path))
        elif os.path.isdir(wallet_path):
            errors.append("Keystore '{}' is a directory instead of a file".format(wallet_path))
    else:
        raise RuntimeError("Only waves crypto is supported now")

    if len(errors) > 0:
        errors_str = '{}{}'.format('- ', '\n- '.join(errors))
        raise RuntimeError(
            "Launcher environment check has failed: config file '{}' has errors:\n{}".format(
                config_file, errors_str))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s] %(message)s')
    logger = logging.getLogger(__name__)

    logger.info('Namespace: %s' % namespace)
    logger.info('Hostname: %s' % hostname)
    logger.info('Vault URL: %s' % vault_url)
    logger.info('Set configurations path to {}'.format(config_path))

    validate_crypto(conf, config_path)

    prepare_node(exec_app)

    default_options = [
        ('-XX:+', 'AlwaysPreTouch'),
        ('-XX:+', 'PerfDisableSharedMem')
    ]

    node_class = 'com.wavesenterprise.Application'
    generator_class = 'com.wavesenterprise.generator.GeneratorLauncher'

    run_cmd(exec_app, node_class, generator_class, default_options)
