# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 16:47:35 2016

prerequisites: pandas, matplotlib
@author: zeroth
"""

import os
import sys
import numpy as np
import tensorflow as tf

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/util")
import data_preproc as preproc
from pbar import PrgBar
import hdfs3
import glob

FLAGS = None


def train_or_predict():
    # FLAGS.input  = 'hdfs://csle1:9000/user/leeyh_etri_re_kr/dataset/input/trainset.csv'
    # FLAGS.output = 'hdfs://csle1:9000/user/leeyh_etri_re_kr/output/models/rnn/0020'
    # FLAGS.model  = '/home/csle/testCodes/models/rnn/0020'

    # FLAGS.input  = 'file:///home/csle/testCodes/input/trainset.csv'
    # FLAGS.output = 'file:///home/csle/testCodes/models/rnn/0020'
    # FLAGS.model  = 'file:///home/csle/testCodes/models/rnn/0020'

    # FLAGS.input  = '/home/csle/testCodes/input/trainset.csv'
    # FLAGS.output = '/home/csle/testCodes/models/rnn/0020'
    # FLAGS.model  = '/home/csle/testCodes/models/rnn/0020'

    (root_path, sep, input_data_path) = FLAGS.input.rpartition('/')
    (checkpoint_dir, sep, model_path) = FLAGS.model.rpartition('/')
    (train_accuracy_dir, sep, train_accuracy_path) = FLAGS.output.rpartition('/')

    if FLAGS.isTrain:
        datasets, num_links = preproc.load_processed_data(FLAGS.isTrain, FLAGS.num_train,
                                                          FLAGS.num_validation,
                                                          FLAGS.num_test,
                                                          FLAGS.input,
                                                          FLAGS.num_steps,
                                                          FLAGS.elapse_steps)
    else:
        datasets, num_links = preproc.load_processed_data(FLAGS.isTrain, FLAGS.num_predict,
                                                          0,
                                                          0,
                                                          FLAGS.input,
                                                          FLAGS.num_steps,
                                                          FLAGS.elapse_steps)
    if (FLAGS.num_links > num_links):
        FLAGS.num_links = num_links
    if (FLAGS.num_outputs > FLAGS.num_links):
        FLAGS.num_outputs = FLAGS.num_links

    if 'sess' in globals():
        sess.close()

    tf.reset_default_graph()
    sess = tf.InteractiveSession()

    x = tf.placeholder(tf.float32,
                       [FLAGS.batch_size, FLAGS.num_steps, FLAGS.num_links],
                       name='input_placeholder')
    y = tf.placeholder(tf.float32, [FLAGS.batch_size, FLAGS.num_outputs],
                       name='labels_placeholder')
    loss_weights = tf.placeholder(tf.float32, [FLAGS.batch_size])
    keep_prob = tf.placeholder(tf.float32)

    cell = tf.contrib.rnn.BasicLSTMCell(
        FLAGS.state_size,
        state_is_tuple=True,
        reuse=tf.get_variable_scope().reuse)

    init_state = cell.zero_state(FLAGS.batch_size, tf.float32)
    rnn_outputs, final_state = tf.nn.dynamic_rnn(cell, x, initial_state=init_state)
    rnn_last_outputs = rnn_outputs[:, FLAGS.num_steps - 1, :]

    # Output Layers
    with tf.variable_scope('fully_connected_0'):
        W = tf.get_variable('W', [FLAGS.state_size, FLAGS.state_size])
        b = tf.get_variable('b', [FLAGS.state_size], initializer=tf.constant_initializer(0.0))

        fc_outputs_0 = tf.nn.elu(tf.matmul(rnn_last_outputs, W) + b)
        fc_dropout_0 = tf.nn.dropout(fc_outputs_0, keep_prob)

    with tf.variable_scope('fully_connected_1'):
        W = tf.get_variable('W', [FLAGS.state_size, FLAGS.state_size])
        b = tf.get_variable('b', [FLAGS.state_size], initializer=tf.constant_initializer(0.0))

        fc_outputs_1 = tf.nn.elu(tf.matmul(fc_dropout_0, W) + b)
        fc_dropout_1 = tf.nn.dropout(fc_outputs_1, keep_prob)

    with tf.variable_scope('fully_connected_2'):
        W = tf.get_variable('W', [FLAGS.state_size, FLAGS.num_outputs])
        b = tf.get_variable('b', [FLAGS.num_outputs], initializer=tf.constant_initializer(0.0))

        # final outputs, predictions
        outputs = tf.nn.elu(tf.matmul(fc_dropout_1, W) + b)

    unscaled_output = 100 * (outputs + 0.5)

    mse = tf.square(y - outputs)
    mse = tf.reduce_mean(mse, reduction_indices=[1])
    total_loss = tf.reduce_sum(mse * loss_weights)

    train_step = tf.train.AdamOptimizer(FLAGS.learning_rate).minimize(total_loss)

    saver = tf.train.Saver()
    sess.run(tf.initialize_all_variables())

    if FLAGS.isTrain:
        print('Tensorflow train job started !')
        num_examples = FLAGS.num_train - FLAGS.num_steps - FLAGS.elapse_steps + 1
        pbar = PrgBar(FLAGS.num_epoch, num_examples)
        p = os.path.join(FLAGS.model, FLAGS.model_filename)
        local_model_path = p[:p.rindex(os.path.sep)]
        local_path_list = local_model_path.split(os.path.sep)
        if (local_path_list[0].lower() == 'file:'):
            local_model_path = '/' + os.path.join(*local_path_list[3:])
        os.makedirs(local_model_path, exist_ok=True)
        for idx in range(FLAGS.num_epoch):
            last_batch = False
            while (last_batch is False):
                X, Y, last_batch, lw, num_samples = datasets.train.next_batch(
                    FLAGS.batch_size, FLAGS.num_links, FLAGS.num_outputs)
                loss_, last_outputs, est, _ = \
                    sess.run([total_loss, rnn_last_outputs, unscaled_output, train_step], \
                             feed_dict={x: X, y: Y, loss_weights: lw, keep_prob: FLAGS.dropout})
                pbar.log(num_samples, loss_)
                sys.stdout.flush()

            if (idx + 1) % FLAGS.checkpoint_steps == 0:
                saver.save(sess, p)
        print(pbar.losses)
        if (idx + 1) % FLAGS.checkpoint_steps != 0:
            saver.save(sess, p)

        model_path = FLAGS.model
        path_list = model_path.split(os.path.sep)
        if (path_list[0].lower() == 'file:'):
            model_path = '/' + os.path.join(*path_list[3:])

        filenames = glob.glob(model_path + '/*')  # /home/csle/testCodes/models/rnn/0019/*

        output_file_path = FLAGS.output
        path_list = output_file_path.split(os.path.sep)
        if (path_list[0].lower() == 'hdfs:'):
            # 'hdfs://csle1:9000/user/leeyh_etri_re_kr/output/models/rnn/0019'
            master, port = path_list[2].split(':')
            hdfs = hdfs3.HDFileSystem(master, port=int(port), user='csle')
            output_path = '/' + os.path.join(*path_list[3:])
            if (hdfs.exists(output_path)):
                hdfs.rm(output_path)
            for file in filenames:
                hdfs.mkdir(output_path)
                path, filename = os.path.split(file)
                hdfs.put(file, output_path + '/' + filename, block_size=1048576)
                print(hdfs.ls(output_path))

        p = os.path.join(train_accuracy_dir, train_accuracy_path)
        local_p = os.path.join(local_model_path, train_accuracy_path)
        accuracy_file = open(local_p, 'w')
        accuracy_file.write("%.7f" % pbar.getAverageLoss())
        accuracy_file.close()
        hdfs.put(local_p, output_path + '/' + train_accuracy_path, block_size=1048576)

    else:
        # Here's where you're restoring the variables w and b.
        # Note that the graph is exactly as it was when the variables were
        # saved in a prior training run.
        print('Tensorflow prediction job started !')
        ckpt = tf.train.get_checkpoint_state(FLAGS.model)
        if ckpt and ckpt.model_checkpoint_path:
            saver.restore(sess, ckpt.model_checkpoint_path)
            print('Restored!', end="\r")
            predictions = evaluate_network(sess,
                                           datasets.train, x, y, total_loss, unscaled_output, loss_weights, keep_prob)

            #             if not os.path.exists(os.path.join(FLAGS.output_predict_dir, FLAGS.output_predict_path)):
            p = os.path.join(FLAGS.output_predict_dir, FLAGS.output_predict_path)
            os.makedirs(p[:p.rindex(os.path.sep)], exist_ok=True)
            predict_file = open(p, 'w')
            for predicts in predictions:
                predicts[predicts < 0] = 0.0
                strings = ["%.2f" % predict for predict in predicts]
                predict_file.write(",".join(strings))
                predict_file.write("\n")
                print(",".join(strings), end="\r")
                sys.stdout.flush()
            predict_file.close()
        else:
            print('No checkpoint found!')


def evaluate_network(sess, dataset, x, y, total_loss, unscaled_output, loss_weights, keep_prob):
    num_examples = dataset.num_examples
    labels = np.zeros((num_examples - FLAGS.num_steps + 1, FLAGS.num_outputs))
    predictions = np.zeros((num_examples - FLAGS.num_steps + 1, FLAGS.num_outputs))
    loss = 0
    count_samples = 0
    count_batches = 0
    last_batch = False
    while (last_batch is False):
        X, Y, last_batch, lw, num_samples = dataset.next_batch(
            FLAGS.batch_size, FLAGS.num_links, FLAGS.num_outputs)
        loss_, pred = sess.run([total_loss, unscaled_output], \
                               feed_dict={x: X, y: Y, loss_weights: lw, keep_prob: FLAGS.dropout})

        loss = loss + loss_

        labels[count_samples:count_samples + num_samples] = Y[0:num_samples]
        predictions[count_samples:count_samples + num_samples] = pred[0:num_samples]
        count_samples += num_samples
        count_batches += 1
    loss = loss / count_batches
    labels = labels[:count_samples]
    predictions = predictions[:count_samples]
    return predictions


def plot_status(sess, datasets, x, y, total_loss, unscaled_output, loss_weights, keep_prob):
    train_loss, train_labels, train_predictions = evaluate_network(
        sess, datasets.train, x, y, total_loss, unscaled_output, loss_weights, keep_prob)
    validation_loss, validation_labels, validation_predictions = evaluate_network(
        sess, datasets.validation, x, y, total_loss, unscaled_output, loss_weights, keep_prob)
    test_loss, test_labels, test_predictions = evaluate_network(
        sess, datasets.test, x, y, total_loss, unscaled_output, loss_weights, keep_prob)
    print(train_loss, validation_loss, test_loss)


def main(_):
    if tf.gfile.Exists(FLAGS.log_dir):
        tf.gfile.DeleteRecursively(FLAGS.log_dir)
    tf.gfile.MakeDirs(FLAGS.log_dir)
    train_or_predict()


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='Train rnn on tensorflow.')
    parser.add_argument('--input', type=str, default="", help='input path')
    parser.add_argument('--output', type=str, default="", help='output path')
    parser.add_argument('--model', type=str, default="", help='model path')

    parser.add_argument('--isTrain', type=bool, default=False,
                        help='If true, train model. If not, predict with model')
    parser.add_argument('--num_links', type=int, default=1400, help='# of cols')
    parser.add_argument('--num_outputs', type=int, default=1400, help='# of outputs')
    parser.add_argument('--num_steps', type=int, default=24, help='# of steps')
    parser.add_argument('--elapse_steps', type=int, default=0, help='next step')
    parser.add_argument('--batch_size', type=int, default=1, help='batch size')
    parser.add_argument('--num_stacked_rnns', type=int, default=3, help='# of stacked rnn layers')
    parser.add_argument('--state_size', type=int, default=128, help='# of hidden units')
    parser.add_argument('--learning_rate', type=float, default=10e-4, help='learning rate')
    parser.add_argument('--checkpoint_steps', type=int, default=1, help='checkpoint steps')
    parser.add_argument(
        '--log_dir', type=str, default='/tmp/csle-tf/logs/rnn', help='Directory to put the log data.')

    # for train
    parser.add_argument('--num_epoch', type=int, default=3, help='# of epochs')
    parser.add_argument('--num_train', type=int, default=10000, help='# of train samples')
    parser.add_argument('--num_validation', type=int, default=2000, help='# of validation samples')
    parser.add_argument('--num_test', type=int, default=3000, help='# of test samples')
    parser.add_argument('--dropout', type=float, default=0.5, help='drop rate')
    parser.add_argument('--model_filename', type=str, default='model.ckpt', help='destination model filename')

    # for predict
    parser.add_argument('--num_predict', type=int, default=0, help='# of predict samples')
    parser.add_argument('--output_predict_dir', type=str, default='/tmp/csle-tf/predict/rnn',
                        help='predicted result directory')
    parser.add_argument('--output_predict_path', type=str, default='predict_rnn.csv',
                        help='predicted result file')

    FLAGS, unparsed = parser.parse_known_args()

    if not FLAGS.isTrain:
        FLAGS.elapse_steps = 0

    tf.app.run()
