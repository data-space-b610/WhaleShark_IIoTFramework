import numpy
import keras

from keras.models import Sequential
from keras.layers import Dense, Flatten
from keras.layers import Activation
from keras.layers import Conv2D

from PIL import Image


def model_set():
    model = Sequential()
    model.add(Conv2D(16, (8, 8), strides=(4, 4), input_shape=(168, 168, 1)))
    model.add(Activation('relu'))

    model.add(Conv2D(32, (4, 4), strides=(2, 2)))
    model.add(Activation('relu'))

    model.add(Conv2D(64, (3, 3), strides=(1, 1)))
    model.add(Activation('relu'))

    model.add(Flatten())
    model.add(Dense(512))
    model.add(Activation('relu'))

    model.add(Dense(4))  # 3-leg or 4-leg
    model.add(Activation('linear'))

    keras.optimizers.RMSprop(lr=0.00025, rho=0.9, epsilon=1e-08)
    model.compile(loss='mean_squared_error', optimizer='adam')

    return model


def convert_bitmap(image_path):
    img = Image.open(image_path)
    A = numpy.array(img)
    A = numpy.expand_dims(A, 0)
    A = numpy.expand_dims(A, 3)

    return A


def run(model, image_path):
    S_ = convert_bitmap(image_path)

    Q = model.predict(S_, batch_size=1, verbose=0) #Q(current S)
    ndx = Q.argmax() # Select a(t)

    return ndx


if __name__ == '__main__':
    image_path = './jamsil4.png'
    weight_path = './tensorflow_model_weights.npy'

    model = model_set()  # Q Deepnet
    model.summary()
    model.load_weights(weight_path)

    # Output ndx value.
    print(run(model, image_path))
