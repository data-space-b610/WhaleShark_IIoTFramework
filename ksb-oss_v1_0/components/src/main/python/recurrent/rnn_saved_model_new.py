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
import tensorflow_util as tfutil

# import pathresolver package
import pathResolver as pathRes

FLAGS = None

def train_or_predict():
    (train_accuracy_dir, sep, train_accuracy_path) = FLAGS.output.rpartition('/')

    if FLAGS.isTrain:
        # Initiate pathresolver
        pathresolver = pathRes.PathResolver(FLAGS.input, FLAGS.modelPath, FLAGS.model, FLAGS.output)

        # get dataframe from input path
        data = pathresolver.get_input_dataframe()

        # Get paths from pathresolver
        local_model_base_path, local_model_path, local_checkpoint_file, output_file_path = \
            pathresolver.get_paths()

        num_links = data.shape[1]
        num_train = FLAGS.num_train
        num_prev = FLAGS.num_steps
        num_next = FLAGS.elapse_steps
        num_validation = FLAGS.num_validation
        if FLAGS.num_train == 0:
            num_train = data.shape[0]

        if num_train < num_prev + num_next:
            print("lack of train samples.")
            return None, 0

        train_data = data.iloc[0:num_train]
        validation_data = data.iloc[num_train:num_train + num_validation]
        test_data = data.iloc[num_train + num_validation:]
        train_dataset = preproc.DataSet(train_data, num_prev, num_next)
        validation_dataset = preproc.DataSet(validation_data, num_prev, num_next)
        test_dataset = preproc.DataSet(test_data, num_prev, num_next)

        class DataSets(object):
            pass

        datasets = DataSets()
        datasets.train = train_dataset
        datasets.validation = validation_dataset
        datasets.test = test_dataset

    if (FLAGS.num_links > num_links):
        FLAGS.num_links = num_links
    if (FLAGS.num_outputs > FLAGS.num_links):
        FLAGS.num_outputs = FLAGS.num_links

    tf.reset_default_graph()
    sess = tf.Session()

    x = tf.placeholder(tf.float32,
                       [FLAGS.batch_size, FLAGS.num_steps, FLAGS.num_links],
                       name='input_placeholder')
    y = tf.placeholder(tf.float32, [FLAGS.batch_size, FLAGS.num_outputs],
                       name='labels_placeholder')
    loss_weights = tf.placeholder(tf.float32, [FLAGS.batch_size])

    keep_prob = tf.constant(FLAGS.dropout, name="keep_prob")

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

        outputs = tf.nn.elu(tf.matmul(fc_dropout_1, W) + b, name="output")

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

        for idx in range(FLAGS.num_epoch):
            last_batch = False
            while (last_batch is False):
                X, Y, last_batch, lw, num_samples = datasets.train.next_batch(
                    FLAGS.batch_size, FLAGS.num_links, FLAGS.num_outputs)
                loss_, last_outputs, est, _ = \
                    sess.run([total_loss, rnn_last_outputs, unscaled_output, train_step], \
                    feed_dict={x:X, y:Y, loss_weights:lw})
                pbar.log(num_samples, loss_)
                sys.stdout.flush()

            if (idx + 1) % FLAGS.checkpoint_steps == 0:
                saver.save(sess, local_checkpoint_file)
        print(pbar.losses)
        if (idx + 1) % FLAGS.checkpoint_steps != 0:
            saver.save(sess, local_checkpoint_file)

        # export model as saved model to ...
        tfutil.export_model(local_model_path,
                            FLAGS.hdfs_user,
                            FLAGS.signature_name,
                            sess, {"in": x}, {"out": outputs})

        local_p = os.path.join(local_model_path, 'accuracy')
        accuracy_file = open(local_p, 'w')
        accuracy_file.write("%.7f" % pbar.getAverageLoss())
        accuracy_file.close()

        # Store model to target file system
        pathresolver.store_output_model()

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
    parser.add_argument('--modelPath', type=str, default="", help='model base path')
    parser.add_argument('--isTrain', type=bool, default=False,
                        help='If true, train model. If not, predict with model')
    parser.add_argument('--num_epoch', type=int, default=3, help='# of epochs')


    FLAGS, unparsed = parser.parse_known_args()
    FLAGS.num_links = 1382
    FLAGS.num_outputs = 1382
    FLAGS.num_steps = 24
    FLAGS.elapse_steps = 0
    FLAGS.batch_size = 1
    FLAGS.num_stacked_rnns = 3
    FLAGS.state_size = 128
    FLAGS.learning_rate = 10e-4
    FLAGS.checkpoint_steps = 1
    FLAGS.log_dir = '/tmp/csle-tf/logs/rnn'
    FLAGS.num_train = 10000
    FLAGS.num_validation = 2000
    FLAGS.num_test = 3000
    FLAGS.dropout = 0.5
    FLAGS.hdfs_user = "csle"
    FLAGS.signature_name = "predict_speed"

    if not FLAGS.isTrain:
        FLAGS.elapse_steps = 0

    tf.app.run()