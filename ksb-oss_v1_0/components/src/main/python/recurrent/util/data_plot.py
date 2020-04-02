# -*- coding: utf-8 -*-
"""
Created on Tue Aug  9 16:22:11 2016

@author: Administrator
"""

import numpy as np
import matplotlib.pyplot as plt

def grid_scatter(labels, predictions, size, figsize):

    num_rows, num_cols = size
    plt.figure(figsize=figsize)
    for r in range(num_rows):
        for c in range(num_cols):
            inx = r*num_cols + c
            plt.subplot(num_rows, num_cols, inx +1)
     
            target = labels[:, inx]
            pred = predictions[:,inx]
            _min = np.min([np.min(target), np.min(pred)])
            _max = np.max([np.max(target), np.max(pred)])
            plt.plot([_min, _max], [_min, _max], color='r', linestyle='-', linewidth=1)
            plt.scatter(target, pred)
    plt.show()

def grid_plot(labels, predictions, size, figsize, num_samples):
           
    num_rows, num_cols = size
    plt.figure(figsize=figsize)
    for r in range(num_rows):
        for c in range(num_cols):
            inx = r*num_cols + c
            plt.subplot(num_rows, num_cols, inx +1)
     
            target = labels[:, inx][:num_samples]
            pred = predictions[:,inx][:num_samples]

            plt.plot(target, color='b', linestyle='-', linewidth=1)
            plt.plot(pred, color='r', linestyle='-', linewidth=1)
    plt.show()

def grid_plot_diff(labels, predictions, size, figsize, num_samples):
           
    num_rows, num_cols = size
    plt.figure(figsize=figsize)
    for r in range(num_rows):
        for c in range(num_cols):
            inx = r*num_cols + c
            plt.subplot(num_rows, num_cols, inx +1)
     
            target = labels[:, inx][:num_samples]
            pred = predictions[:,inx][:num_samples]
            plt.plot(target-pred, color='b', linestyle='-', linewidth=2)
    plt.show()
