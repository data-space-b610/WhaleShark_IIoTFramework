import os
import pickle
import shutil
import logging
import tempfile
import subprocess

from typing import List


class Dockerize:
    """
    Build a docker image for Python script.
    """

    def __init__(self, tag: str, src_folder: str,
                 requirements: List[str]=None, user_args=None) -> None:
        """
        Initialize.

        Args:
            tag (str): Docker tag.
            src_folder (str): Folder containing python files and data files.
            requirements (List[str]): (optional) PIP installable package names.
            user_args (str): (optional) This parameter will be saved as
                "ksb_user_args.pkl", and can be accessed from a user-provided
                Python script. See the ksblib.dockerize document for details.
        """

        # Set logger.
        log_format = '%(asctime)s [%(filename)s:%(lineno)s] ' + \
                     '%(levelname)s - %(message)s'
        logging.basicConfig(format=log_format, level=logging.INFO)
        logger = logging.getLogger(__name__)
        self.logger = logger

        self.tag = tag
        self.src_folder = src_folder
        if requirements is None:
            self.requirements = []
        else:
            self.requirements = requirements
        self.user_args = user_args

    # TODO: reader setting.
    def _set_reader(self):
        pass

    # TODO: writer setting.
    def _set_writer(self):
        pass

    def build(self):
        """
        Build a docker image.
        """

        # Create a temp folder to build custom docker image.
        self.logger.info('Create working folder.')
        temp_dir = tempfile.TemporaryDirectory()

        # Copy template files into the temp folder.
        self.logger.info('Copy template files into the working folder: {0}'.
                         format(temp_dir.name))
        docker_template_path = os.path.join(os.path.dirname(__file__),
                                            'template')
        src_docker_file = os.path.join(docker_template_path, 'Dockerfile')
        src_require_file = os.path.join(docker_template_path,
                                        'requirements.txt')
        src_run_file = os.path.join(docker_template_path, 'run.py')

        # Copy each necessary file individually.
        # DO NOT USE shutil.copytree since there are some files and folders
        # that should not be copied.
        shutil.copy(src_docker_file, temp_dir.name)
        shutil.copy(src_require_file, temp_dir.name)
        shutil.copy(src_run_file, temp_dir.name)

        # Copy custom files from src_folder into the docker build folder.
        self.logger.info('Copy user-provided files into the working folder.')

        # If HDFS file.
        module_folder = os.path.join(temp_dir.name, 'modules')
        if self.src_folder.strip().startswith('hdfs://'):
            self.logger.info('Copy files from HDFS.')
            # TODO: replace this with hdfs3 library.
            p = subprocess.Popen(['hdfs', 'dfs', '-get', self.src_folder,
                                  module_folder], stdout=subprocess.PIPE)
            for line in p.stdout:
                self.logger.info('HDFS | {0}'.format(
                    line.decode().replace(os.linesep, '')))

        # If local file.
        else:
            # Remove file prefix if exists.
            # This prefix comes from KSB framework.
            file_prefix = 'file://'
            if self.src_folder.strip().startswith(file_prefix):
                self.src_folder = self.src_folder.replace(file_prefix, '')

            shutil.copytree(self.src_folder, module_folder)

        # Edit requirements contents.
        dst_require_file = os.path.join(temp_dir.name,
                                        os.path.basename(src_require_file))
        fp = open(dst_require_file, 'a')

        # From function argument.
        for package in self.requirements:
            fp.writelines('{0}\n'.format(package))

        # From user-provide "requirements.txt"
        user_require = os.path.join(module_folder, 'requirements.txt')
        if os.path.isfile(user_require):
            user_fp = open(user_require, 'r')
            for package in user_fp:
                fp.writelines('{0}\n'.format(package))
            user_fp.close()
        fp.close()

        # Put ksb_user_args.pkl
        fp = open(os.path.join(module_folder, 'ksb_user_args.pkl'), 'wb')
        pickle.dump(self.user_args, fp)
        fp.close()

        # Build docker.
        self.logger.info('Build a docker image on the working folder.')
        os.chdir(temp_dir.name)
        # TODO: replace this with docker library.
        p = subprocess.Popen(['docker', 'build', '-t', self.tag, '.'],
                             stdout=subprocess.PIPE)
        for line in p.stdout:
            self.logger.info('Docker | {0}'.format(
                line.decode().replace(os.linesep, '')))

    # TODO: Send "user_args" as input parameter?
    def run(self, port=8080):
        """
        Run the build Docker image.

        Args:
            port (int): Host port for accepting incoming requests.
        """

        self.logger.info('Run {0}.'.format(self.tag))
        # We fix internal port to 8080. This can be changed to any port number.
        # TODO: replace this with docker library.
        p = subprocess.Popen(['docker', 'run', '-p',
                              '{0}:8080'.format(port), self.tag],
                             stdout=subprocess.PIPE)
        for line in p.stdout:
            self.logger.info(line.decode().replace(os.linesep, ''))
