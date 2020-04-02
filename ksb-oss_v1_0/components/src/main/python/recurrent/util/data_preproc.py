# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 10:46:00 2016

@author: zeroth
"""
import datetime
import os
import pickle

import numpy as np
import pandas as pd
import hdfs3



def save_raw_data_as_pickle(raw_files, pkl_files, path='.'):
# Header: PRCS_YEAR, PRCS_MON, PRCS_DAY, PRCS_HH, PRCS_MIN, LINK_ID, PRCS_SPD, PRCS_TRV_TIME
    for raw_file, pkl_file in zip(raw_files, pkl_files):
        data = pd.read_csv(path + '//' + raw_file)
        pickle.dump(data, open(path + '//' + pkl_file, 'wb'))

def read_data_file(input_file_path, max_row=None):
    # input_file_path = 'file:///home/csle/testCodes/input/trainset.csv'
    # input_file_path = '/home/csle/testCodes/input/trainset.csv'
    data = None
    num_rows_to_read = max_row  # if max_row is None, it will read all files
    path, filename = os.path.split(input_file_path)
    print(path, filename)
    path_list = input_file_path.split(os.path.sep)
    if(path_list[0].lower() == 'file:'):
        input_file_path = '/' + os.path.join(*path_list[3:])

    print(input_file_path)
    ext = filename.split('.')[-1]

    if ext in ['txt', 'csv'] :
        data = pd.read_csv(input_file_path, nrows=num_rows_to_read, header=None)
    elif ext in ['pkl', 'p']:
        data = pd.read_pickle(input_file_path)
    num_rows = data.shape[0]
    if max_row is not None and num_rows >= max_row:
        data = data.iloc[:max_row]
    return data

def read_data_file_from_hdfs(input_file_path, max_row=None):

    # input_file_path = 'hdfs://csle1:9000/user/leeyh_etri_re_kr/dataset/input/trainset.csv'
    num_rows_to_read = max_row  # if max_row is None, it will read all files
    # path, filename = os.path.split(input_file_path)
    # print(path, filename)
    path_list = input_file_path.split(os.path.sep)
    print(path_list[0], path_list[2], path_list[4])
    master, port = path_list[2].split(':')
    print(master, port)
    hdfs = hdfs3.HDFileSystem(master, port=int(port), user= path_list[4])
    input_file_path = '/' + os.path.join(*path_list[3:])
    with hdfs.open(input_file_path) as f:
        data = pd.read_csv(f, nrows=num_rows_to_read, header=None)

    num_rows = data.shape[0]
    if max_row is not None and num_rows >= max_row:
        data = data.iloc[:max_row]
    return data

def read_data_files(files, path=None, max_row=None):
    if path is None:
        path = './/'
    data = None
    num_rows_to_read = max_row  # if max_row is None, it will read all files
    for file in files:
        ext = file.split('.')[-1]
        if ext in ['txt', 'csv'] :
            df = pd.read_csv(path + '//' + file, nrows=num_rows_to_read, header=None)
        elif ext in ['pkl', 'p']:
            df = pd.read_pickle(path + '//' + file)
        if data is None:
            data = df
        else:
            data = data.append(df, ignore_index=True)
        num_rows = data.shape[0]
        if max_row is not None and num_rows >= max_row:
            data = data.iloc[:max_row]
            break
        if max_row is not None:
            num_rows_to_read = max_row - num_rows
    return data

def reshape_data(data):
    data['PRCS_DATE_TIME'] = ''
    for c in data.columns[1:5]:
        data['PRCS_DATE_TIME'] = data['PRCS_DATE_TIME'] + data[c].apply(lambda x: '%02d' % x)
    data['PRCS_DATE_TIME'] = data['PRCS_YEAR'].map(str) + data['PRCS_DATE_TIME']
    reshaped = data.pivot(index='PRCS_DATE_TIME', columns='LINK_ID', values='PRCS_SPD')
    del data
    # data.drop(['PRCS_DATE_TIME'], inplace=True, axis=1)
    reshaped.dropna(axis=1, how='any', inplace=True)
    reshaped.sort_index(inplace=True)
    reshaped.reset_index(level=0, inplace=True)
    date_time = pd.DataFrame(reshaped['PRCS_DATE_TIME'])
    reshaped.drop(['PRCS_DATE_TIME'], inplace=True, axis=1)

    date_time['YEAR'] = date_time['PRCS_DATE_TIME'].apply(lambda x: int(x[0:4]))
    date_time['MON'] = date_time['PRCS_DATE_TIME'].apply(lambda x: int(x[4:6]))
    date_time['DAY'] = date_time['PRCS_DATE_TIME'].apply(lambda x:int(x[6:8]))
    date_time['HOUR'] = date_time['PRCS_DATE_TIME'].apply(lambda x: int(x[8:10]))
    date_time['MIN'] = date_time['PRCS_DATE_TIME'].apply(lambda x: int(x[10:12]))
    date_time['WDAY'] = date_time.apply(
        lambda x: datetime.date(x['YEAR'], x['MON'], x['DAY']).weekday(), axis=1)
    date_time.drop(['PRCS_DATE_TIME'], inplace=True, axis=1)
    reshaped = pd.concat([date_time, reshaped], axis=1)
    return reshaped

def reshape_save_as_pickle(raw_files, pkl_files, path):
   for raw_file, pkl_file in zip(raw_files, pkl_files):
        data = pd.read_csv(path + '//' + raw_file)
        data = reshape_data(data)
        pickle.dump(data, open(path + '//' + pkl_file, 'wb'))

def form_in_out(data, num_prev, num_next, num_links, num_outputs):
    X, Y = [], []
    num_rows = len(data)
    num_iter = num_rows - num_prev - num_next + 1
    if num_iter < 1:
        return
    for i in range(num_iter):
        X.append(np.array([d[:num_links] for d in data[i:i + num_prev]]))
        Y.append(np.array(data[i + num_prev + num_next - 1][:num_outputs]))
    X = np.array(X)
    Y = np.array(Y)
    return X, Y

def aggregate(data, win_size):
    num_date_time_headers = 6

def zero_pad(X, num, axis):
    num_dims = len(X.shape)
    pad_width = []
    for d in range(num_dims):
        if axis != d:
            pad_width.append((0, 0))
        else:
            pad_width.append((0, num))
    pad_X = np.pad(X, pad_width, mode='constant', constant_values=0)
    return pad_X

class DataSet(object):
    def __init__(self, data, num_prev, num_next, date_included=False):
        num_date_headers = 6
        if date_included is True:
            self._data = data.values[:, num_date_headers:]
        else:
            self._data = data.values
        self._data = np.ndarray.astype(self._data, dtype='float32')
        self._num_examples = data.shape[0]
        self._num_prev = num_prev
        self._num_next = num_next
        self._index_in_epoch = 0
        self._epochs_completed = 0
    @property
    def images(self):
        pass

    @property
    def labels(self):
        pass

    @property
    def num_examples(self):
        return self._num_examples

    @property
    def eopchs_completed(self):
        return self._epochs_completed

    def next_batch(self, batch_size, num_links, num_outputs):
        start = self._index_in_epoch
        if start + self._num_prev + self._num_next > self._num_examples:
            # Finished epoch
            self._epochs_completed += 1
            # Start next epoch
            start = 0
            self._index_in_epoch = 0
        end = min(start + batch_size + self._num_prev + self._num_next - 1, self._num_examples)
        X, Y = form_in_out(self._data[start:end], self._num_prev, self._num_next, num_links, num_outputs)
        self._index_in_epoch = min(start + batch_size, self._num_examples)
        last_batch = end == self._num_examples
        num_samples = Y.shape[0]
        loss_weight = np.ones(shape=(num_samples,), dtype=Y.dtype)
        if num_samples < batch_size:
            X = zero_pad(X, batch_size - num_samples, axis=0)
            Y = zero_pad(Y, batch_size - num_samples, axis=0)
            loss_weight = zero_pad(loss_weight, batch_size - num_samples, axis=0)
        loss_weight = loss_weight / num_samples

        return X, Y, last_batch, loss_weight, num_samples

def load_data(num_train, num_validation, num_test, num_prev=10, num_next=0):

    # num_train = 1500; num_validation = 200; num_test = 300
    max_num = num_train + num_validation + num_test
    path = './'
    raw_files = ('201509.txt', '201510.txt', '201511.txt', '201512.txt')
    pkl_files = ('201509.pkl', '201510.pkl', '201511.pkl', '201512.pkl')
    # save_raw_data_as_pickle(raw_files, pkl_files, path)
    # reshape_save_as_pickle(raw_files, pkl_files, path)
    data = read_data_files(pkl_files, path, max_row=max_num)
    data.dropna(axis=1, how='any', inplace=True)

    num_links = data.shape[1] - 6
    train_data = data.iloc[0:num_train]
    validation_data = data.iloc[num_train:num_train + num_validation]
    test_data = data.iloc[num_train + num_validation:]
    train_dataset = DataSet(train_data, num_prev, num_next)
    validation_dataset = DataSet(validation_data, num_prev, num_next)
    test_dataset = DataSet(test_data, num_prev, num_next)

    class DataSets(object):
        pass

    data_sets = DataSets()
    data_sets.train = train_dataset
    data_sets.validation = validation_dataset
    data_sets.test = test_dataset

    return data_sets, num_links

def pklToCsv(inPath, pkl_files, outPath, path_trainset, path_predictset, cols, num_train=15000, num_predict=10):
    max_num = num_train + num_predict
    data = read_data_files(pkl_files, inPath, max_row=max_num)
    num_date_headers = 6
    data = data.iloc[:, num_date_headers:]
    data.dropna(axis=1, how='any', inplace=True)
    data = data / 100 - 0.5

    p = os.path.join(outPath, path_trainset)
    os.makedirs(p[:p.rindex(os.path.sep)], exist_ok=True)
    data.iloc[:num_train, :cols].to_csv(p, header=None, index=None, sep=',', mode='a')

    p = os.path.join(outPath, path_predictset)
    os.makedirs(p[:p.rindex(os.path.sep)], exist_ok=True)
    data.iloc[num_train:, :cols].to_csv(p, header=None, index=None, sep=',', mode='a')

def load_processed_data(isTrain, num_train, num_validation, num_test, input_file_path,
                        num_prev=25, num_next=0):
    prefix = input_file_path.split(os.path.sep)
    if(prefix[0].lower() == 'hdfs:'):
        data = read_data_file_from_hdfs(input_file_path, max_row=None)
    else:
        data = read_data_file(input_file_path, max_row=None)

    num_links = data.shape[1]

    if num_train == 0:
        num_train = data.shape[0]
    if isTrain:
        if num_train < num_prev + num_next:
            print("lack of train samples.")
            return None, 0
    else:
        if num_train < num_prev:
            print("lack of train samples.")
            return None, 0

    train_data = data.iloc[0:num_train]
    validation_data = data.iloc[num_train:num_train + num_validation]
    test_data = data.iloc[num_train + num_validation:]
    train_dataset = DataSet(train_data, num_prev, num_next)
    validation_dataset = DataSet(validation_data, num_prev, num_next)
    test_dataset = DataSet(test_data, num_prev, num_next)

    class DataSets(object):
        pass

    data_sets = DataSets()
    data_sets.train = train_dataset
    data_sets.validation = validation_dataset
    data_sets.test = test_dataset

    return data_sets, num_links

if __name__ == '__main__':
    inPath = '../examples'
    outPath = '/tmp/csle-tf/datasets/rnn'
#     raw_files = ('201509.txt', '201510.txt', '201511.txt', '201512.txt')
#     pkl_files = ('201509.pkl', '201510.pkl', '201511.pkl', '201512.pkl')
    pkl_files = ('201510.pkl','201510.pkl')
    path_trainset = ('trainset.csv')
    path_predictset = ('predictset.csv')
    num_train = 15000
    num_predict = 26
    cols = 1360

    if os.path.isfile(os.path.join(outPath, path_trainset)):
        os.remove(os.path.join(outPath, path_trainset))
    if os.path.isfile(os.path.join(outPath, path_predictset)):
        os.remove(os.path.join(outPath, path_predictset))
    pklToCsv(inPath, pkl_files, outPath, path_trainset, path_predictset, cols, num_train, num_predict)

