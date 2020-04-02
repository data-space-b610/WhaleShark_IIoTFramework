
import numpy as np
import matplotlib.pyplot as plt


def generate_wave(input_num=5, noise_loc=-1):
    t = np.arange(0.0, 13.0, 0.01)
    inputs = []
    target = np.sin(0)  # initialize to 0
    for i in range(input_num):
        inputs.append(np.sin(np.pi * t * (i+1)))
        if i == noise_loc:
            inputs[i] = add_noise(inputs[i])  # add noise
        target += (i+1) * inputs[i]
    return np.column_stack(inputs + [target])  # seq_len x dim


def add_noise(input):
    t_noise = np.arange(0.0, 0.5, 0.01)
    noise = 1.7 * np.cos(np.pi * t_noise * 10)
    #insert = np.random.randint(1001, 1200) ## soh
    insert = int(round(1100))
    input[insert:insert + 50] = input[insert:insert + 50] + noise
    return input


def normalize_data(data):
    for index in range(data.shape[1]):
        mean = data[:, index].mean()
        std = data[:, index].std()
        data[:, index] -= mean
        data[:, index] /= std
    return data


def make_seq_data(X, Y, seq_len):
    dataX = []
    dataY = []
    for i in range(0, Y.shape[0] - seq_len + 1):
        _x = X[i:i + seq_len]
        _y = Y[i + seq_len - 1]
        dataX.append(_x)
        dataY.append(_y)
    return dataX, dataY


def draw_data(data):
    try:
        plt.figure(1)
        for i in range(data.shape[1]-1):
            num = 100 * (data.shape[1]) + 10 + (i+1)
            plt.subplot(num)
            plt.title("Input " + str(i+1))
            plt.plot(data[:, i], 'gray')
        num = 101 * (data.shape[1]) + 10
        plt.subplot(num)
        plt.title("target")
        plt.plot(data[:, -1], 'r')
        plt.show()
    except Exception as e:
        print("plotting exception")
        print(str(e))


def generate_data(input_num, noise_loc, seq_len):
    data = generate_wave(input_num, noise_loc)
    #draw_data(data)
    data = normalize_data(data)

    train_X, val_X, test_X= data[:700, :-1], data[700:1000, :-1], data[1000:, :-1]
    train_Y, val_Y, test_Y = data[:700, [-1]], data[700:1000, [-1]], data[1000:, [-1]]

    trainX,trainY = make_seq_data(train_X, train_Y, seq_len)
    valX,valY = make_seq_data(val_X, val_Y, seq_len)
    testX,testY = make_seq_data(test_X, test_Y, seq_len)

    trainX, valX, testX = np.array(trainX), np.array(valX), np.array(testX)
    trainY, valY, testY = np.array(trainY), np.array(valY), np.array(testY)

    return trainX, trainY, valX, valY, testX, testY


def generate_data_test():
    INPUT_NUM = 5 # input dimension
    NOISE_LOC = 2 # 0-4, noise insert location
    SEQ_LEN = 10  # LSTM step-length

    trainX, trainY, valX, valY, testX, testY = generate_data(input_num=INPUT_NUM, noise_loc=NOISE_LOC, seq_len=SEQ_LEN)

    print("train X.shape=",trainX.shape, ", trainY.shape=",trainY.shape)
    print("testX.shape=",testX.shape, ", testY.shape=",testY.shape)
    print("valX.shape=", valX.shape, ", valY.shape=",valY.shape)

# generate_data_test()

# # 20180405@sewonoh
# def save_data_test():
#     INPUT_NUM = 5 # input dimension
#     NOISE_LOC = 2 # 0-4, noise insert location
#     SEQ_LEN = 10  # LSTM step-length
#
#     _, _, _, _, testX, testY = generate_data(input_num=INPUT_NUM, noise_loc=NOISE_LOC, seq_len=SEQ_LEN)
#
#
#     testX_res = testX.reshape(-1,SEQ_LEN*INPUT_NUM)
#     testY_res = testY.reshape(-1,1)
#
#     print(np.shape(testX_res))
#     print(np.shape(testY_res))
#
#     np.savetxt("./data/testX.csv", testX_res[:1], delimiter=",")
#     np.savetxt("./data/testY.csv", testY_res[:1], delimiter=",")
#
# # save_data_test()
#
# # 20180405@sewonoh
# def load_data_test():
#     INPUT_NUM = 5 # input dimension
#     SEQ_LEN = 10  # LSTM step-length
#
#     testX_res = np.loadtxt("./data/testX.csv", delimiter=",")
#     testY_res = np.loadtxt("./data/testY.csv", delimiter=",")
#
#     print(np.shape(testX_res))
#     print(np.shape(testY_res))
#
#     testX = testX_res.reshape(-1, SEQ_LEN, INPUT_NUM)
#     testY = testY_res.reshape(-1, 1)
#
#     print(np.shape(testX))
#     print(np.shape(testY))
#
# # load_data_test()


# 20180515@sewonoh
if __name__ == '__main__':
    def save_data_test_csle():
        INPUT_NUM = 5 # input dimension
        NOISE_LOC = 2 # 0-4, noise insert location

        data = generate_wave(INPUT_NUM, NOISE_LOC)
        data = normalize_data(data)

        train_X = data[:700, :-1]
        train_Y = data[:700, [-1]]
        test_X = data[1000:1010, :-1]
        test_Y = data[1000:1010, [-1]]

        trainX = np.array(train_X)
        trainY = np.array(train_Y)
        testX = np.array(test_X)
        testY = np.array(test_Y)

        print(np.shape(trainX))
        print(np.shape(trainY))
        print(np.shape(testX))
        print(np.shape(testY))

        np.savetxt("./data/train_x.csv", trainX, delimiter=",")
        np.savetxt("./data/train_y.csv", trainY, delimiter=",")
        np.savetxt("./data/test_x.csv", testX, delimiter=",")
        np.savetxt("./data/test_y.csv", testY, delimiter=",")

    save_data_test_csle()