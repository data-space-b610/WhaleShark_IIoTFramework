# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 09:20:34 2016

@author: zeroth
"""
import time
import datetime
import math
import numpy as np

class PrgBar:

    
    def __init__(self, num_epochs, num_examples, bar_length=20):
        
        self._num_epochs = num_epochs
        self._num_examples = num_examples
        self._bar_length = bar_length

        self._num_trained_samples_in_epoch = 0
        self._epoch = 0      

        self._global_start = 0
            
        self._epoch_start = 0            
        self._losses = []
        
        self.new_epoch()
        
    def new_epoch(self):

        start = time.time()                  
        
        if self._epoch == 0:
            self._global_start = start
            
        self._epoch_start = start            
        self._epoch += 1
        self._num_trained_samples_in_epoch = 0
        
    def log(self, num_trained_samples, loss):

        if isinstance(loss, (list, tuple)):
            if self._epoch == 1 and self._num_trained_samples_in_epoch == 0:
                self._losses = [[] for i in range(len(loss))]
        
        self._num_trained_samples_in_epoch += num_trained_samples
        
        if self._num_trained_samples_in_epoch > self._num_examples:
            self._num_trained_samples_in_epoch = self._num_examples
        
        
        if isinstance(loss, (list, tuple)):
            for i in range(len(loss)):
                self._losses[i].append(loss[i])                        
        else:
            self._losses.append(loss)


        self.display()
        
        if self._num_trained_samples_in_epoch >= self._num_examples:
            print('')
            self.new_epoch()
                
    @property        
    def losses(self):
        return self._losses
        
    def increase(self, num_samples):
        self._num_trained_samples_in_epoch += num_samples
        
        if self._num_trained_samples_in_epoch > self._num_examples:
            self._num_trained_samples_in_epoch = self._num_examples


    def estimate_epoch(self):
        
        num_trained = self._num_trained_samples_in_epoch
        epoch_start = self._epoch_start
        num_examples = self._num_examples
        prg = num_trained / num_examples
        
        elpsed_time = (time.time() - epoch_start)
        est_time = elpsed_time/prg
        
        return est_time, elpsed_time


    def estimate_global(self):
        
        num_trained = self._num_trained_samples_in_epoch
        num_examples = self._num_examples
        epoch = self._epoch
        num_epochs = self._num_epochs
        
        global_prg = (num_trained/num_examples + epoch-1) / num_epochs
        global_start = self._global_start

        
        elpsed_time = math.floor((time.time() - global_start))
        est_time = math.floor(elpsed_time/global_prg)
        
               
        
        return est_time, elpsed_time

        
    def display(self, num_trained=None, epoch=None):        
        
        if num_trained is None:
            num_trained = self._num_trained_samples_in_epoch
            
        if epoch is None: 
            epoch = self._epoch
            
        num_examples = self._num_examples
        bar_length = self._bar_length
        num_epochs = self._num_epochs
        
        epoch_est, epoch_elp = self.estimate_epoch()
        global_est, global_elp = self.estimate_global()
        

        prg = num_trained / num_examples
    
        bar_length = bar_length - 1
        num_prg_bars = int(prg * bar_length)
        prg_bar = '[' + '-'*(num_prg_bars) + '>' + '.' *(bar_length-num_prg_bars) + ']'
        
        epoch_time = (' %.2f' % epoch_elp) + '/' + ('%.2f' % (epoch_est)) 
        
        global_time = str(datetime.timedelta(seconds=global_elp)) + '/'
        global_time += str(datetime.timedelta(seconds=global_est))
      
      
        
        end_time = time.localtime(self._global_start + global_est)
        global_end = time.strftime('%d %b %Y %H:%M:%S', end_time)
        
        #'%2d.%2d.%4d-%2d:%2d' % (date.tm_mon, date.tm_mday, date.tm_year, date.tm_hour, date.tm_min)
        
        
        num_digits = math.floor(math.log(num_epochs, 10))+1     
        epoch_prg = ('%' + str(num_digits) + 'd')%(epoch) + '/' + str(num_epochs) 
        print('\r', epoch_prg, prg_bar, epoch_time, ',', global_time, ',', global_end, ' ', end='')
    
    
        loss = []
        if isinstance(self._losses[0], (list, tuple)):        
            loss = [l[-100:] for l in self._losses]
            print("Avg. loss for last %d steps:" %(len(loss[0])), np.mean(loss, axis=1), end='')
        else:
            loss = self._losses[-100:]
            print("Avg. loss for last %d steps:" %(len(loss)), np.mean(loss), end='')

    def getAverageLoss(self):
        loss = []
        avgLoss = 0
        if isinstance(self._losses[0], (list, tuple)):        
            loss = [l[-100:] for l in self._losses]
            avgLoss = np.mean(loss, axis=1)
        else:
            loss = self._losses[-100:]
            avgLoss = np.mean(loss)
        
        return avgLoss        

        
if __name__ == '__main__':
    
    num_epochs = 5
    num_examples = 1000
    num_batches = 10
    
    pbar = PrgBar(num_epochs, num_examples)
    
    
    for epoch in range(num_epochs):

        #pbar.new_epoch()
        for batch in range(num_batches):
            batch_size = num_examples / num_batches

            #Training...            
            time.sleep(1)
            
            pbar.log(batch_size, [1, 2, 3])
            #pbar.display()
        
#def progress_bar(start, epoch, num_epochs, num_trained, num_examples, bar_length = 20):
#        
#    elpsed_time = (time.time() - start)
#    prg = num_trained / num_examples
#    
#    nb_prg_bars = int(prg * bar_length)
#    prg_bar = '[' + '-'*(nb_prg_bars-1) + '>' + '.' *(bar_length-nb_prg_bars) + ']'
#    prg_bar = prg_bar + (' %.2f' % elpsed_time) + '/' + ('%.2f' % (elpsed_time/prg)) + ' '
#    
#    nb_digits = math.floor(math.log(num_epochs, 10))+1     
#    f = '%' + str(nb_digits) + 'd' 
#    print('\r', f%(epoch+1), '/', num_epochs, prg_bar, end='')
#    
#    if num_trained >= num_examples:
#        print('')
#       
#        
#
#
#if __name__ == '__main__':
#    
#    num_epochs = 100
#    num_examples = 1000
#    num_batches = 10
#    
#    
#    
#    for epoch in range(num_epochs):
#
#        num_trained = 0
#        start = time.time()
#
#        for batch in range(num_batches):
#            batch_size = num_examples / num_batches
#            
#            num_trained += batch_size        
#            time.sleep(1)
#            
#            progress_bar(start, epoch, num_epochs, num_trained, num_examples, bar_length = 20)
        
        
        
    
    
        