# ===================================================
# Modified 2018.05.15
# Since 2018.03.15
# Edited by sewonoh@etri.re.kr
# Based on modelGRU.py(mariekim@etri.re.kr, 20180213)
# ===================================================

import os
os.environ['TF_CPP_MIN_LOG_LEVEL']='2'

import numpy as np
import tensorflow as tf

FLAGS = None


def variable_summaries(var):
    """Attach a lot of summaries to a Tensor (for TensorBoard visualization).

    Args:
        var: tensorflow variable.

    Returns:
        None.
    """

    with tf.name_scope('summaries'):
        mean = tf.reduce_mean(var)
        tf.summary.scalar('mean', mean)
        with tf.name_scope('stddev'):
            stddev = tf.sqrt(tf.reduce_mean(tf.square(var - mean)))
        tf.summary.scalar('stddev', stddev)
        tf.summary.scalar('max', tf.reduce_max(var))
        tf.summary.scalar('min', tf.reduce_min(var))
        tf.summary.histogram('histogram', var)


def rnn_cell(hidden_dim, keep_prob):
    """RNN Cell.

    Args:
        hidden_dim: number of hidden node
        keep_prob: drop-out probability

    Returns:
        rnn cell
    """

    cell = tf.nn.rnn_cell.GRUCell(num_units=hidden_dim, activation=tf.tanh)
    cell = tf.contrib.rnn.DropoutWrapper(cell, output_keep_prob=keep_prob)
    return cell


def trainModel(X, output_dim=1, keep_prob=1.0, train_mode=0):
    """Train Model.
    Note: The number of hidden node should be appropriately adjusted.

    Args:
        X: input tensor
        output_dim: output tensor dimension. default=1
        keep_prob: drop-out probability. default=1.0
        train_mode: 1 means True; 0 means False. default=0

    Returns:
        Y_pred
    """

    HIDDEN_DIM = FLAGS.NUM_HIDDEN_NODE

    with tf.name_scope('trainModel'):
        with tf.variable_scope('cells'):
            if train_mode is 1: tf.get_variable_scope().reuse_variables()
            multi_cells = tf.nn.rnn_cell.MultiRNNCell([rnn_cell(HIDDEN_DIM, keep_prob) for _ in range(1)])

        outputs, _states = tf.nn.dynamic_rnn(multi_cells, X, dtype=tf.float32)
        Y_pred = tf.contrib.layers.fully_connected(outputs[:, -1], output_dim, activation_fn=None)
        return Y_pred


def train(trainX, trainY):
    """Train TF model.

    Args:
        trainX: independent variables
        trainY: dependent variable

    Returns:
        None.
    """

    SEQ_LENGTH  = trainX.shape[1]
    DATA_DIM    = trainX.shape[2]
    OUTPUT_DIM  = FLAGS.OUTPUT_LEN

    with tf.name_scope('input'):
        X = tf.placeholder(tf.float32, [None, SEQ_LENGTH, DATA_DIM], name="X")
        Y = tf.placeholder(tf.float32, [None, OUTPUT_DIM], name="Y")

    keep_prob = tf.Variable(initial_value=FLAGS.KEEP_PROB, trainable=False)
    train_mode = tf.Variable(initial_value=1, trainable=False)

    Y_pred = trainModel(X, OUTPUT_DIM, keep_prob, train_mode)

    with tf.name_scope('loss'):
        loss = tf.reduce_mean(tf.squared_difference(Y_pred, Y))
        variable_summaries(loss)

    # for causal analysis.
    weights = tf.trainable_variables()
    weights_norm = tf.norm(tf.concat((weights[0][0:DATA_DIM,:], weights[2][0:DATA_DIM,:]), axis=1), axis=1)
    with tf.name_scope('gradients'):
        gradients = tf.gradients(loss, weights, name="gradients")
    grads_norm = tf.norm(tf.concat((gradients[0][0:DATA_DIM, :], gradients[2][0:DATA_DIM, :]), axis=1), axis=1)
    grads_argmax = tf.argmax(grads_norm)

    # for train.
    with tf.name_scope('train_op'):
        train_op = tf.train.RMSPropOptimizer(learning_rate=FLAGS.LEARNING_RATE).minimize(loss)

    # for validate.
    rmse = tf.sqrt(loss)

    # before exporting final model.
    keep_prob = tf.assign(keep_prob, 1.0)
    train_mode = tf.assign(train_mode, 0)

    merged = tf.summary.merge_all()
    saver = tf.train.Saver()

    with tf.Session() as sess:

        sess.run(tf.global_variables_initializer())

        if tf.gfile.Exists(FLAGS.LOG_DIR):
            tf.gfile.DeleteRecursively(FLAGS.LOG_DIR)
        tf.gfile.MakeDirs(FLAGS.LOG_DIR)
        writer = tf.summary.FileWriter(FLAGS.LOG_DIR, sess.graph)

        train_losses = 0.
        for i in range(FLAGS.MAX_EPOCH):
            train_loss, _, summary = sess.run([loss, train_op, merged], feed_dict={X: trainX, Y: trainY})
            train_losses += train_loss
            writer.add_summary(summary, i)

        writer.close()
        avg_train_loss = train_losses / FLAGS.MAX_EPOCH
        print(">>> Avg. train loss: {}".format(avg_train_loss))

        sess.run([keep_prob, train_mode])
        save_path = saver.save(sess, FLAGS.SAVE_PATH)
        print("Model saved in file: %s from train()" % save_path)


        ################################
        ### tensorflow serving model ###
        ################################
        from urllib.parse import urlparse
        from os.path import basename, join, exists
        import shutil
        def _to_proto(tensors):
            protos = {}
            for k, v in tensors.items():
                protos[k] = tf.saved_model.utils.build_tensor_info(v)
            return protos


        parsed_url = urlparse(FLAGS.EXPORT_URL)
        base_path = parsed_url.path

        model_version = FLAGS.MODEL_VERSION
        model_path = join(base_path, model_version)
        if exists(model_path):
            shutil.rmtree(model_path)
        builder = tf.saved_model.builder.SavedModelBuilder(model_path)

        ## signature predict
        signature_name1 = "predict"
        input_tensors = {"in": X}
        output_tensors = {"out": Y_pred}
        signature1 = tf.saved_model.signature_def_utils.build_signature_def(
            inputs=_to_proto(input_tensors),
            outputs=_to_proto(output_tensors),
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
        )

        ## signature validate
        signature_name2 = "validate"
        input_tensors = {"in": X, "y": Y}
        output_tensors = {"rmse": rmse}
        signature2 = tf.saved_model.signature_def_utils.build_signature_def(
            inputs=_to_proto(input_tensors),
            outputs=_to_proto(output_tensors),
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
        )

        ## signature do_ca
        signature_name3 = "do_ca"
        input_tensors = {"in": X, "y": Y}
        output_tensors = {"gradients": grads_norm, "result": grads_argmax, "weights": weights_norm}
        signature3 = tf.saved_model.signature_def_utils.build_signature_def(
            inputs=_to_proto(input_tensors),
            outputs=_to_proto(output_tensors),
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
        )

        builder.add_meta_graph_and_variables(
            sess,
            tags=[tf.saved_model.tag_constants.SERVING],
            signature_def_map={
                signature_name1: signature1,
                signature_name2: signature2,
                signature_name3: signature3
            },
            assets_collection=None,
            legacy_init_op=None,
            clear_devices=None,
            main_op=None)
        builder.save(as_text=False)
        print("Serving Model saved in file : %s from train()" % FLAGS.EXPORT_URL)

        sess.close()


def predict(testX):
    """Predict.

    Args:
        testX: independent variables for prediction

    Returns:
        None.

    """
    tf.reset_default_graph()

    SEQ_LENGTH  = testX.shape[1]
    DATA_MIN    = testX.shape[2]

    X = tf.placeholder(tf.float32, [None, SEQ_LENGTH, DATA_MIN], name="X")
    Y_pred = trainModel(X)

    saver = tf.train.Saver()

    with tf.Session() as sess:
        saver.restore(sess, FLAGS.SAVE_PATH)
        print("\nModel loaded from %s for predict()" % FLAGS.SAVE_PATH)

        y_pred = sess.run(Y_pred, feed_dict={X: testX})
        np.set_printoptions(formatter={'float': '{: 0.4f}'.format})
        y_pred_pp = np.reshape(y_pred, -1)
        print(">>> Y_pred = ", y_pred_pp)
        return y_pred


def validate(valX, valY):
    """Validate.

    Args:
        valX: independent variables for validation
        valY: dependent variables for validation

    Returns:
        None.
    """
    tf.reset_default_graph()

    SEQ_LENGTH  = valX.shape[1]
    DATA_DIM    = valX.shape[2]
    OUTPUT_DIM  = FLAGS.OUTPUT_LEN

    X = tf.placeholder(tf.float32, [None, SEQ_LENGTH, DATA_DIM], name="X")
    Y = tf.placeholder(tf.float32, [None, OUTPUT_DIM], name="Y")
    Y_pred = trainModel(X, output_dim=OUTPUT_DIM)
    rmse = tf.sqrt(tf.reduce_sum(tf.square(Y_pred - Y)))

    saver = tf.train.Saver()

    with tf.Session() as sess:
        saver.restore(sess, FLAGS.SAVE_PATH)
        print("\nModel loaded from %s for validate()" % FLAGS.SAVE_PATH)

        rmse_result = sess.run([rmse], feed_dict={X: valX, Y: valY})
        print(">>> RMSE = {}".format(rmse_result))

        return rmse_result


def do_ca(testX, testY):
    """Causal Analysis.

    Args:
        testX: independent variables for causal analysis
        testY: dependent variables for causal analysis

    Returns:
        None.
    """
    tf.reset_default_graph()

    SEQ_LENGTH  = testX.shape[1]
    DATA_DIM    = testX.shape[2]
    OUTPUT_DIM  = FLAGS.OUTPUT_LEN

    X = tf.placeholder(tf.float32, [None, SEQ_LENGTH, DATA_DIM], name="X")
    Y = tf.placeholder(tf.float32, [None, OUTPUT_DIM], name="Y")
    Y_pred = trainModel(X, output_dim=OUTPUT_DIM)

    mse = tf.square(Y_pred - Y)

    weights = tf.trainable_variables()
    weights_norm = tf.norm(tf.concat((weights[0][0:DATA_DIM, :], weights[2][0:DATA_DIM, :]), axis=1), axis=1)
    gradients = tf.gradients(mse, weights, name="gradients")
    grads_norm = tf.norm(tf.concat((gradients[0][0:DATA_DIM, :], gradients[2][0:DATA_DIM, :]), axis=1), axis=1)
    grads_argmax = tf.argmax(grads_norm)

    saver = tf.train.Saver()

    with tf.Session() as sess:
        saver.restore(sess, FLAGS.SAVE_PATH)
        print("\nModel loaded from %s for do_ca()" % FLAGS.SAVE_PATH)

        gr, ga = sess.run([grads_norm, grads_argmax], feed_dict={X: testX, Y: testY})
        print(">>> Anomaly = %d" % ga)
        np.set_printoptions(formatter={'float': '{: 0.4f}'.format})
        print(">>> Gradients = ", gr)

        return ga, gr


def main(_):
    """Main function

    Args:
        None.

    Returns:
        None.

    """
    print(FLAGS.PRINT_DEBUG_SEPARATOR + " Start of main() >>>")
    tf.set_random_seed(0)

    ############################
    ### test data generation ###
    ############################
    if FLAGS.USE_DEMO_DATA:
        from generateData import generate_data
        X_train, Y_train, _, _, X_test, Y_test = generate_data(FLAGS.INPUT_NUM, FLAGS.NOISE_LOC, FLAGS.SEQ_LEN)
        print("X_train.shape=", X_train.shape, ", Y_train.shape=", Y_train.shape)
        print("X_test.shape=", X_test.shape, ", Y_test.shape=", Y_test.shape)

        ## For quick testing, the data amount are reduced purposedly.
        X_test, Y_test = X_test[0:5,:,:], Y_test[0:5, :]
        print("X_test.shape=", X_test.shape, ", Y_test.shape=", Y_test.shape)

        X_pred, X_val, X_ca = X_test, X_test, X_test
        Y_val, Y_ca = Y_test, Y_test

    else:
        from generateData import make_seq_data

        if FLAGS.DO_TRAIN:
            if not tf.gfile.Exists(FLAGS.TRAIN_DATA_X_PATH):
                exit("error TRAIN_DATA_X_PATH")
            if not tf.gfile.Exists(FLAGS.TRAIN_DATA_Y_PATH):
                exit("error TRAIN_DATA_Y_PATH")
            data_X = np.genfromtxt(FLAGS.TRAIN_DATA_X_PATH, delimiter=',')
            data_Y = np.genfromtxt(FLAGS.TRAIN_DATA_Y_PATH, delimiter=',')
            X_train, Y_train = make_seq_data(data_X, data_Y, FLAGS.SEQ_LEN)
            X_train, Y_train = np.array(X_train), np.array(Y_train)
            Y_train = np.reshape(Y_train, (-1, 1))
            print("X_train.shape=", X_train.shape, ", Y_train.shape=", Y_train.shape)

        if FLAGS.DO_PREDICT:
            if not tf.gfile.Exists(FLAGS.PREDICT_DATA_X_PATH):
                exit("error PREDICT_DATA_X_PATH")
            data_X = np.genfromtxt(FLAGS.PREDICT_DATA_X_PATH, delimiter=',')
            X_pred = []
            for i in range(0, data_X.shape[0] - FLAGS.SEQ_LEN + 1):
                _x = data_X[i:i + FLAGS.SEQ_LEN]
                X_pred.append(_x)
            X_pred = np.array(X_pred)
        if FLAGS.DO_VALIDATE:
            if not tf.gfile.Exists(FLAGS.VALIDATE_DATA_X_PATH):
                exit("error VALIDATE_DATA_X_PATH")
            if not tf.gfile.Exists(FLAGS.VALIDATE_DATA_Y_PATH):
                exit("error VALIDATE_DATA_Y_PATH")
            data_X = np.genfromtxt(FLAGS.VALIDATE_DATA_X_PATH, delimiter=',')
            data_Y = np.genfromtxt(FLAGS.VALIDATE_DATA_Y_PATH, delimiter=',')
            X_val, Y_val = make_seq_data(data_X, data_Y, FLAGS.SEQ_LEN)
            X_val, Y_val = np.array(X_val), np.array(Y_val)
            Y_val = np.reshape(Y_val, (-1, 1))
        if FLAGS.DO_CA:
            if not tf.gfile.Exists(FLAGS.CA_DATA_X_PATH):
                exit("error CA_DATA_X_PATH")
            if not tf.gfile.Exists(FLAGS.CA_DATA_Y_PATH):
                exit("error Ca_DATA_Y_PATH")
            data_X = np.genfromtxt(FLAGS.CA_DATA_X_PATH, delimiter=',')
            data_Y = np.genfromtxt(FLAGS.CA_DATA_Y_PATH, delimiter=',')
            X_ca, Y_ca = make_seq_data(data_X, data_Y, FLAGS.SEQ_LEN)
            X_ca, Y_ca = np.array(X_ca), np.array(Y_ca)
            Y_ca = np.reshape(Y_ca, (-1, 1))


    #################
    ### functions ###
    #################
    print(FLAGS.PRINT_DEBUG_SEPARATOR + " train")
    if FLAGS.DO_TRAIN:
        train(X_train, Y_train)

    print(FLAGS.PRINT_DEBUG_SEPARATOR + " predict")
    if FLAGS.DO_PREDICT:
        predict(X_pred)

    print(FLAGS.PRINT_DEBUG_SEPARATOR + " validate")
    if FLAGS.DO_VALIDATE:
        validate(X_val, Y_val)

    print(FLAGS.PRINT_DEBUG_SEPARATOR + " do_ca")
    if FLAGS.DO_CA:
        do_ca(X_ca, Y_ca)

    print(FLAGS.PRINT_DEBUG_SEPARATOR + " End of main() <<<")


if __name__ == '__main__':
    import time
    import argparse

    parser = argparse.ArgumentParser(description='Anomaly2017 on tensorflow.')
    parser.register('type', 'bool', (lambda x: x.lower() in ("yes", "true", "t", "1")))

    # about execution
    parser.add_argument('--DO_TRAIN',       type='bool', default=True, help='execute Train')
    parser.add_argument('--DO_PREDICT',     type='bool', default=True, help='execute Prediction')
    parser.add_argument('--DO_VALIDATE',    type='bool', default=True, help='execute Validation')
    parser.add_argument('--DO_CA',          type='bool', default=True, help='execute Causal Analysis')

    # about data shape and data path
    parser.add_argument('--SEQ_LEN',        type=int, default=1, help='GRU step-length (number of rows)')
    ## parser.add_argument('--OUTPUT_LEN',     type=int, default=1, help='output length (number of rows)')

    ## Note: If USE_DEMO_DATA==True, then xxx_DATA_PATH will be ignored.
    parser.add_argument('--USE_DEMO_DATA',      type='bool', default=True, help='use demo data (wave combinations)')
    parser.add_argument('--INPUT_NUM',          type=int, default=5, help='number of input wave(s)')
    parser.add_argument('--NOISE_LOC',          type=int, default=2, help='0-(INPUT_NUM-1); noise will be inserted to the selected wave')

    parser.add_argument('--TRAIN_DATA_X_PATH',      type=str, default='./data/train_x.csv', help='train data x file path')
    parser.add_argument('--TRAIN_DATA_Y_PATH',      type=str, default='./data/train_y.csv', help='train data y file path')
    parser.add_argument('--PREDICT_DATA_X_PATH',    type=str, default='./data/test_x.csv',  help='predict data x file path')
    parser.add_argument('--VALIDATE_DATA_X_PATH',   type=str, default='./data/test_x.csv',   help='validation data x file path')
    parser.add_argument('--VALIDATE_DATA_Y_PATH',   type=str, default='./data/test_y.csv',   help='validation data y file path')
    parser.add_argument('--CA_DATA_X_PATH',         type=str, default='./data/test_x.csv',    help='ca data x file path')
    parser.add_argument('--CA_DATA_Y_PATH',         type=str, default='./data/test_y.csv',    help='ca data y file path')

    # about training parameters
    parser.add_argument('--LEARNING_RATE',  type=float, default=0.007, help='learning rate')
    parser.add_argument('--MAX_EPOCH',      type=int,   default=5, help='max epoch')
    parser.add_argument('--KEEP_PROB',      type=float, default=0.7, help='dropout keep probability')
    parser.add_argument('--NUM_HIDDEN_NODE',type=int,   default=24, help='number of hidden nodes')

    # for logging and saving model
    parser.add_argument('--LOG_DIR',        type=str, default='./tf_logs/', help='log directory path')
    parser.add_argument("--SAVE_PATH",      type=str, default="/tmp/anomaly_2017.ckpt", help="save path")

    # for serving model
    parser.add_argument("--EXPORT_URL",     type=str, default="/tmp/anomaly2017", help="url to export a saved model")
    parser.add_argument("--MODEL_VERSION",  type=str, default="1", help="model version")


    FLAGS, unparsed = parser.parse_known_args()

    PRINT_DEBUG_SEPARATOR = "#" * 50
    for i in range(0, 5): print('')
    print(PRINT_DEBUG_SEPARATOR)
    print(__file__)
    print("Executed at " + time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    print("\nFLAGS " + "=" * 20)
    print(FLAGS)
    print("\nunparsed "+ "=" * 17)
    print(unparsed)
    FLAGS.PRINT_DEBUG_SEPARATOR = PRINT_DEBUG_SEPARATOR
    FLAGS.OUTPUT_LEN = 1

    tf.app.run()


