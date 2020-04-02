import os
import sys
import shutil
import argparse

# sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/util')
import numpy as np
import pandas as pd
import tensorflow as tf
import pathResolver as pathRes


class DataLoader:
    def __init__(self, data):
        """
        DataLoader class.

        Args:
            data (pandas.DataFrame): Data in Pandas DataFrame format.
        """

        self.data = data

        # User-customized data.
        self.inputs = None
        self.outputs = None
        self.prepare_data()

    def prepare_data(self):
        """
        Prepare data for training a graph.

        Returns:
            Input/output data in numpy array.
        """

        # Convert the first column to datetime type.
        self.data.ix[:, 0] = pd.to_datetime(self.data.ix[:, 0])

        # Sort by the date.
        self.data = self.data.sort_values(0)

        # Get 8 rows as input, and set the row three after as output.
        data_np = self.data.values[::, 1:]
        inputs = []
        outputs = []
        for i in range(len(data_np) - 8 - 3):
            input = data_np[i:i + 8].flatten()
            output = data_np[i + 8 + 3].flatten()

            inputs.append(input)
            outputs.append(output)

        inputs = np.array(inputs)
        outputs = np.array(outputs)
        inputs = inputs.reshape(-1, 8, 170)

        self.inputs = inputs
        self.outputs = outputs

    def get_batch(self, batch_size=64, shuffle=True):
        """
        Get batch samples of the shuffled dataset.

        Args:
            batch_size (int): Batch size.
            shuffle (boolean): If shuffle or not.

        Returns:
            Sample of batch size.
        """

        # Shuffle data.
        if shuffle:
            shuffle_index = np.arange(len(self.inputs))
            np.random.shuffle(shuffle_index)

            shuffled_inputs = self.inputs[shuffle_index]
            shuffled_outputs = self.outputs[shuffle_index]
        else:
            shuffled_inputs = self.inputs
            shuffled_outputs = self.outputs

        # Enumerate.
        n_iter = int(shuffled_inputs.shape[0] / batch_size)
        for i in range(n_iter):
            yield shuffled_inputs[i * batch_size: (i + 1) * batch_size], \
                  shuffled_outputs[i * batch_size: (i + 1) * batch_size]


class TensorFlowModel():
    def __init__(self, local_model_path, local_checkpoint_file):
        """
        Build graph and export a model.
        """
        self.X = None
        self.Y = None
        self.loss = None
        self.train = None
        self.output = None
        self.local_model_path = local_model_path
        self.local_checkpoint_file = local_checkpoint_file

    def build_graph(self):
        """
        Build a graph
        """

        # Define inputs and outputs
        seq_length = 8
        num_links = 170
        hidden_size = 128
        learning_rate = 0.01
        self.X = tf.placeholder(tf.float32, shape = [None, seq_length, num_links])
        self.Y = tf.placeholder(tf.float32, shape = [None, num_links])

        # build a LSTM network
        cell = tf.contrib.rnn.BasicLSTMCell(
            num_units=hidden_size, state_is_tuple=True, activation=tf.tanh)
        outputs, _states = tf.nn.dynamic_rnn(cell, self.X, dtype=tf.float32)
        Y_pred = tf.contrib.layers.fully_connected(
            outputs[:, -1], num_links, activation_fn=None)  # We use the last cell's output

        # cost/loss
        self.loss = tf.reduce_sum(tf.square(Y_pred - self.Y))  # sum of the squares

        # optimizer
        optimizer = tf.train.AdamOptimizer(learning_rate)
        self.train = optimizer.minimize(self.loss)

        self.output = Y_pred


    def train_graph(self, loader):
        """
        Train a graph
        """

        saver = tf.train.Saver()

        with tf.Session() as sess:
            init = tf.global_variables_initializer()
            sess.run(init)

            # restore from checkpoint file
            if os.path.exists(self.local_checkpoint_file):
                ckpt_path = tf.train.latest_checkpoint(self.local_checkpoint_file)
                saver.restore(sess, ckpt_path)
                print("Model Restored from ", ckpt_path)


            num_epochs = FLAGS.num_epochs
            num_batch = 10
            for i in range(num_epochs):
                batch_loader = loader.get_batch(batch_size=num_batch, shuffle=False)
                for inputs, outputs in batch_loader:
                    _, loss = sess.run([self.train, self.loss],
                                 feed_dict={self.X: inputs, self.Y: outputs})

                print("train lose: ", loss)

            # checkpoint save
            saver.save(sess, self.local_checkpoint_file)
            # tensorflow serving model export
            self.export(sess)
            sess.close()

        tf.reset_default_graph()


    def export(self, session):
        """
        Export a model.
        """
        export_dir_path = self.local_model_path

        if os.path.exists(export_dir_path):
            shutil.rmtree(export_dir_path)

        builder = tf.saved_model.builder.SavedModelBuilder(export_dir_path)

        signature = tf.saved_model.signature_def_utils.build_signature_def(
            inputs={
                "in1": tf.saved_model.utils.build_tensor_info(self.X)
            },
            outputs={
                "out1": tf.saved_model.utils.build_tensor_info(self.output)
            },
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
        )
        builder.add_meta_graph_and_variables(
            session,
            tags=[tf.saved_model.tag_constants.SERVING],
            signature_def_map={"predict_speed": signature},
            assets_collection=None,
            legacy_init_op=None,
            clear_devices=None,
            main_op=None)
        builder.save()
        print('exported model in ', export_dir_path)


def main():
    # Preparation of data.
    pathresolver = pathRes.PathResolver(FLAGS.input, FLAGS.modelPath,
                                        FLAGS.model, FLAGS.output)
    _, local_model_path, local_checkpoint_file, _ = pathresolver.get_paths()
    data = pathresolver.get_input_dataframe()
    loader = DataLoader(data)

    if FLAGS.isTrain:
        # Train a model and export.
        tf_model = TensorFlowModel(local_model_path, local_checkpoint_file)
        tf_model.build_graph()
        tf_model.train_graph(loader)
        # Store model to target Hdfs
        pathresolver.store_output_model()
    else:
        pass



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train a model on tensorflow.')

    # for train
    parser.add_argument('--input', type=str, default='traffic_processing.csv', help='input path')
    parser.add_argument('--model', type=str, default='file:///home/csle/kangnam/model/0000', help='model path')
    parser.add_argument('--modelPath', type=str, default='file:///home/csle/kangnam/model', help='model base path')
    parser.add_argument('--output', type=str, default='file:///home/csle/kangnam/output', help='output path')

    parser.add_argument('--isTrain', type=bool, default=True,
                        help='If true, train model. If not, predict with model')
    parser.add_argument('--num_epochs', type=int, default=3, help='# of epochs')

    FLAGS, unparsed = parser.parse_known_args()

    main()


