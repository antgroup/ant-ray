import numpy as np
import time

import logging as logger

logger.basicConfig()
logger.root.setLevel(logger.INFO)
logger.basicConfig(level=logger.INFO)


import torch 
from torch import nn
from PIL import Image
import matplotlib.pyplot as plt
import os
from torchvision import datasets, transforms,utils
import torch.nn.functional as F
import torch.optim as optim

from torch.utils.data import DataLoader, DistributedSampler

# For Ray
import ray
from ray import train
import ray.train.torch
from ray.train import Trainer
from ray.train.torch import TorchConfig

import sys
import traceback
import threading
def print_pstack_thread():
    while(True):
        avail_mem = ray._private.utils.estimate_available_memory()
        print(f"Current available memory: {avail_mem}")
        stacks = sys._current_frames()
        i = 1
        for thread_id, stack in stacks.items():
            print(f"[{i} / {len(stacks.items())}]Thread ID: {thread_id}")
            i+=1
            traceback.print_stack(stack)
            print("....")
        print()
        print("... Sleep for 5 seconds ...")
        print()
        time.sleep(60)

# t = threading.Thread(target=print_pstack_thread)
# t.start()

def _draw_train_process(title,iters,costs,accs,label_cost,lable_acc):
    plt.title(title, fontsize=24)
    plt.xlabel("iter", fontsize=20)
    plt.ylabel("acc(\%)", fontsize=20)
    plt.plot(iters, costs,color="red",label=label_cost) 
    plt.plot(iters, accs,color="green",label=lable_acc) 
    plt.legend()
    plt.grid()
    plt.show()


def _prepare_train_data():
    print("[torch-ray]Start to download train data")
    transform = transforms.Compose([transforms.ToTensor(),
                               transforms.Normalize(mean=[0.5],std=[0.5])])
    train_data = datasets.MNIST(root = "/root/data/", # data path in Occlum, instead of the host
                                transform=transform,
                                train = True,
                                download = False)
    print("[torch-ray]Done load train data.")
    train_loader = DataLoader(train_data, batch_size=16)
    train_loader = train.torch.prepare_data_loader(train_loader)
    return train_loader

def _prepare_test_data():
    transform = transforms.Compose([transforms.ToTensor(),
                               transforms.Normalize(mean=[0.5],std=[0.5])])
    test_data = datasets.MNIST(root="/root/data/",
                            transform = transform,
                            train = False)
    test_loader = torch.utils.data.DataLoader(test_data,batch_size=16,
                                            shuffle=True,num_workers=2)
    return test_loader

class CNN(nn.Module):
    def __init__(self):
        super(CNN,self).__init__()
        self.conv1 = nn.Conv2d(1,32,kernel_size=3,stride=1,padding=1)
        self.pool = nn.MaxPool2d(2,2)
        self.conv2 = nn.Conv2d(32,64,kernel_size=3,stride=1,padding=1)
        self.fc1 = nn.Linear(64*7*7,1024)
        self.fc2 = nn.Linear(1024,512)
        self.fc3 = nn.Linear(512,10)
#         self.dp = nn.Dropout(p=0.5)

    def forward(self,x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))

        x = x.view(-1, 64 * 7* 7)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))   
        x = self.fc3(x)  
        return x

def train_func(config):
    print("[torch-ray]Start train_func")
    train_loader = _prepare_train_data()
    print("[torch-ray]Done prepare train data, start to prepare CNN model.") 
    cnn_net = CNN()
    cnn_net = train.torch.prepare_model(cnn_net)
    print("[torch-day]Done prepare CNN model.")

    # 3. Define the loss function.
    criterion = nn.CrossEntropyLoss()
    # 4. Define the SGD optimizer.
    sgd_optimizer = optim.SGD(cnn_net.parameters(), lr=0.001, momentum=0.9)

    train_accs = []
    train_loss = []
    test_accs = []
    # device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # net = net.to(device)
    net = cnn_net
    optimizer = sgd_optimizer

    for epoch in range(4):
        running_loss = 0.0
        print(f"[torch-ray] Current epoch: {epoch}")
        for i,data in enumerate(train_loader, 0):
            print(f"[torch-ray] Epoch {epoch} iterate {i}")
            inputs,labels = data
            # Cleanup the data of the last batch
            optimizer.zero_grad()         
            # forward, backward, optimizing
            outputs = net(inputs)
            print(f"[torch-ray] Network output: {outputs}")
            loss = criterion(outputs,labels)
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            if i % 100 == 99:
                print("[%d,%5d] loss :%.3f" %
                    (epoch+1,i+1,running_loss/100))
                logger.info(f"[{epoch+1}, {i+1}] loss: {running_loss/100}")
                running_loss = 0.0
            train_loss.append(loss.item())
            
            correct = 0
            total = 0
            _, predicted = torch.max(outputs.data, 1)
            total = labels.size(0)
            correct = (predicted == labels).sum().item() # Number of the correct predication items.
            train_accs.append(100*correct/total) 

    print("Finished Training")
    logger.info("Finished Training")
    return net.module


def _prepare_data_and_train():
    start_time = time.time()    
    print("[torch-ray] Start to init `Trainer`")
    trainer = Trainer(backend="torch", num_workers=3)
    print(f"[torch-ray] Done init `Trainer`, prepare to start")
    trainer.start() # set up resources
    print("[torch-ray] Started trainer, prepare to run")

    trainer.run(train_func)
    trainer.shutdown() # clean up resources
    end_time = time.time()
    print("========It took ", end_time - start_time)
    logger.info(f"========It took {end_time - start_time}")

    # trained_net = _train_my_model(cnn_net, train_loader, sgd_optimizer, criterion)
    # path_to_save = "/tmp//raytrain_demo/trainedmodel"
    # torch.save(trained_net.state_dict(), path_to_save)

def _load_model_and_predict():
    test_loader = _prepare_test_data() 
    dataiter = iter(test_loader)
    images, labels = dataiter.next()

    print("GroundTruth: ", " ".join("%d" % labels[j] for j in range(64)))
    test_net = CNN()
    test_net.load_state_dict(torch.load("/root/trainedmodel"))
    test_out = test_net(images)

    print(test_out)
    logger.info(f"test output: {test_out}")
    _, predicted = torch.max(test_out, dim=1)
    print("Predicted: ", " ".join("%d" % predicted[j]
                                for j in range(64)))

ray.init(
    object_store_memory=500 * 1024 * 1024,
    _temp_dir="/host/tmp/ray",
    _plasma_directory="/tmp"
)

_prepare_data_and_train()
