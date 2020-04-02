import logging
import sys
import shutil
import tensorflow as tf

from hdfs import InsecureClient, HdfsError
from os.path import basename, join, exists
from urllib.parse import urlparse


logging.basicConfig(
    format="%(asctime)s [%(levelname)8s] %(message)s (%(name)s: %(lineno)s)",
    level=logging.INFO,
    stream=sys.stdout)


class SavedModelUploader(object):
    """upload a saved model to hadoop file system"""

    def __init__(self, url, user, base_path=""):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._url = url
        self._user_ = user
        self._base_path = base_path
        self._client = InsecureClient(url, user)

        if not self._exist(base_path):
            self._mkdir(base_path)

    def _exist(self, path):
        if self._client.content(path, strict=False):
            return True
        else:
            return False

    def _mkdir(self, path):
        self._client.makedirs(path)

    def _del(self, path):
        self._client.delete(path, recursive=True)

    def _upload(self, local_path, hdfs_path):
        self._client.upload(hdfs_path, local_path)

    def _logging_progress(self, local_path, nbytes):
        msg = None
        if nbytes > 0:
            msg = "uploading: '{}' [{} bytes]".format(local_path, nbytes)
        else:
            msg = "uploading: '{}' [done]".format(local_path)
        self._logger.info(msg)

    def upload(self, local_model_path, overwrite=False):
        hdfs_model_path = self._base_path + '/' + basename(local_model_path)

        existed = self._exist(hdfs_model_path)
        if overwrite and existed:
            self._del(hdfs_model_path)
        elif not overwrite and existed:
            raise RuntimeError("could not overwrite the model, already existed.")

        try:
            self._client.upload(self._base_path, local_model_path,
                                progress=self._logging_progress)
        except HdfsError as e:
            self._logger.error(e)

        self._logger.info("model upload done")


def export_model(url, user, signature_name,
                 sess, input_tensors={}, output_tensors={},
                 legacy_init_op=None,
                 overwrite=True, save_as_text=False):
    _logger = logging.getLogger(__name__)
    _logger.info("export tensorflow model")

    parsed_url = urlparse(url)

    base_path = None
    uploader = None
    if parsed_url.scheme == "http":
        base_path = join("/tmp", parsed_url.path[1:])
        hdfs_url = parsed_url.scheme + "://" + parsed_url.netloc
        uploader = SavedModelUploader(hdfs_url, user, parsed_url.path)
    else:
        base_path = parsed_url.path

    model_path = base_path
    if exists(model_path):
        print("model_path already exists.")
        shutil.rmtree(model_path)

    predict_signature = tf.saved_model.signature_def_utils.build_signature_def(
        inputs=_to_proto(input_tensors),
        outputs=_to_proto(output_tensors),
        method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
    )

    builder = tf.saved_model.builder.SavedModelBuilder(model_path)
    builder.add_meta_graph_and_variables(
        sess,
        tags=[tf.saved_model.tag_constants.SERVING],
        signature_def_map={
            signature_name: predict_signature,
        },
        assets_collection=None,
        legacy_init_op=legacy_init_op,
        clear_devices=None,
        main_op=None)
    builder.save(as_text=save_as_text)

    if uploader:
        uploader.upload(model_path, overwrite)


def _to_proto(tensors):
    protos = {}
    for k, v in tensors.items():
        protos[k] = tf.saved_model.utils.build_tensor_info(v)
    return protos


if __name__ == "__main__":
    pass
