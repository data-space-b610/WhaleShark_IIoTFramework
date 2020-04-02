"""
Added by H.-J. Kim.
"""

import os
import hdfs3
import glob
import shutil
import pandas as pd

class PathResolver:
    """
    Manage paths for tensorflow training.
    """
    def __init__(self, input_file_path, modelPath, model, output):
        """
          Initiate instance using input file path, modelPath(local path), model, output path parameters.
         """
        self.input_file_path = input_file_path
        self.local_model_base_path = modelPath
        self.local_model_path = model
        self.output_file_path = output
        self.local_checkpoint_file = ""

    def get_paths(self):
        """
          Return local model base path, local model path, local checkpoint file path, output file path.
         """
        local_path_list = self.local_model_path.split(os.path.sep)
        if (local_path_list[0].lower() == 'file:'):
            self.local_model_path = '/' + os.path.join(*local_path_list[3:])
        local_path_list = self.local_model_base_path.split(os.path.sep)
        if (local_path_list[0].lower() == 'file:'):
            self.local_model_base_path = '/' + os.path.join(*local_path_list[3:])
        local_checkpoint_file = os.path.join(self.local_model_base_path, 'model.ckpt')
        local_path_list = self.output_file_path.split(os.path.sep)
        if (local_path_list[0].lower() == 'file:'):
            self.output_file_path = '/' + os.path.join(*local_path_list[3:])
        print('local_model_base_path: ', self.local_model_base_path)
        print('local_model_path: ', self.local_model_path)
        print('local_checkpoint_file: ', self.local_checkpoint_file)
        print('output_file_path: ', self.output_file_path)
        os.makedirs(self.local_model_base_path, exist_ok=True)
        return self.local_model_base_path, self.local_model_path, local_checkpoint_file, self.output_file_path

    def store_output_model(self):
        """
          Store tensorflow model stored in local model path to output file path.
        """
        filenames = glob.glob(self.local_model_path + '/*')

        path_list = self.output_file_path.split(os.path.sep)
        if (path_list[0].lower() == 'hdfs:'):
            master, port = path_list[2].split(':')
            hdfs = hdfs3.HDFileSystem(master, port=int(port), user='csle')
            output_path = '/' + os.path.join(*path_list[3:])
            print('local_model_path: {a}'.format(a=self.local_model_path))
            print('output_path: {a}'.format(a=self.output_file_path))
            if (hdfs.exists(output_path)):
                hdfs.rm(output_path)
            for file in filenames:
                hdfs.mkdir(output_path)
                if(os.path.isdir(file)):
                    path, filename = os.path.split(file)
                    hdfs.mkdir(output_path + '/' + filename)
                    filesIn2ndLevelFolder = glob.glob(file + '/*')
                    for fileIn2ndLevelFolder in filesIn2ndLevelFolder:
                        path, filenameIn2ndLevelFolder = os.path.split(fileIn2ndLevelFolder)
                        hdfs.put(fileIn2ndLevelFolder, output_path + '/' + filename + '/' + filenameIn2ndLevelFolder,
                                 block_size=1048576)
                else:
                    path, filename = os.path.split(file)
                    hdfs.put(file, output_path + '/' + filename, block_size=1048576)
        else:
            print("local_model_path: ", self.local_model_path)
            print("output_file_path: ", self.output_file_path)
            shutil.copytree(self.local_model_path, self.output_file_path)

    def _read_data_file(self, input_file_path, max_row=None):
        data = None
        num_rows_to_read = max_row  # if max_row is None, it will read all files
        path, filename = os.path.split(input_file_path)
        path_list = input_file_path.split(os.path.sep)
        if (path_list[0].lower() == 'file:'):
            input_file_path = '/' + os.path.join(*path_list[3:])
        print(input_file_path)
        data = pd.read_csv(input_file_path, nrows=num_rows_to_read, header=None)
        num_rows = data.shape[0]
        if max_row is not None and num_rows >= max_row:
            data = data.iloc[:max_row]
        return data

    def _read_data_file_from_hdfs(self, input_file_path, max_row=None):
        num_rows_to_read = max_row  # if max_row is None, it will read all files
        path_list = input_file_path.split(os.path.sep)
        master, port = path_list[2].split(':')
        hdfs = hdfs3.HDFileSystem(master, port=int(port), user=path_list[4])
        input_file_path = '/' + os.path.join(*path_list[3:])
        with hdfs.open(input_file_path) as f:
            data = pd.read_csv(f, nrows=num_rows_to_read, header=None)
        num_rows = data.shape[0]
        if max_row is not None and num_rows >= max_row:
            data = data.iloc[:max_row]
        return data

    def get_input_dataframe(self):
        """
          Return dataframe after reading training dataset from input file path.
        """
        print("input file path:",self.input_file_path)
        prefix = self.input_file_path.split(os.path.sep)
        if(prefix[0].lower() == 'hdfs:'):
            data = self._read_data_file_from_hdfs(self.input_file_path, max_row=None)
        else:
            data = self._read_data_file(self.input_file_path, max_row=None)
        return data
